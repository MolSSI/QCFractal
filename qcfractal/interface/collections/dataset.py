"""
QCPortal Database ODM
"""
import itertools as it
from enum import Enum
from typing import Any, Dict, List, Tuple, Union, Optional

import numpy as np
import pandas as pd
from pydantic import BaseModel

from .collection import Collection
from .collection_utils import nCr, register_collection
# from .. import client
from .. import constants
from .. import dict_utils
from .. import statistics
from ..models.common_models import Molecule


class Entry(BaseModel):
    name: str
    molecule_id: str
    comment: Optional[str] = None
    manual_results: Dict[str, Any]


class Dataset(Collection):
    """
    The Dataset class for compu.

    Attributes
    ----------
    client : client.FractalClient
        A optional server portal to connect the database
    data : dict
        JSON representation of the database backbone
    df : pd.DataFrame
        The underlying dataframe for the Dataset object
    """

    def __init__(self, name, client=None, **kwargs):
        """
        Initializer for the Dataset object. If no Portal is supplied or the database name
        is not present on the server that the Portal is connected to a blank database will be
        created.

        Parameters
        ----------
        name : str
            The name of the Dataset
        client : client.FractalClient, optional
            A Portal client to connected to a server
        """
        super().__init__(name, client=client, **kwargs)

        # Initialize internal data frames
        self.df = pd.DataFrame(index=self.get_index())

        # If we making a new database we may need new hashes and json objects
        self._new_molecules = {}
        self._new_keywords = {}

    class DataModel(Collection.DataModel):

        # Defaults
        default_program: Optional[str] = None
        default_keywords: Dict[str, str] = {}
        default_driver: str = "energy"
        alias_keywords: Dict[str, Dict[str, str]] = {}

        entries: List[Entry] = []

    def _check_state(self):
        if self._new_molecules or self._new_keywords:
            raise ValueError("New molecules or keywords detected, run save before submitting new tasks.")

    def _pre_save_prep(self, client):

        # Preps any new molecules introduced to the Dataset before storing data.
        mol_ret = client.add_molecules(self._new_molecules)

        # Update internal molecule UUID's to servers UUID's
        self.data.entries = dict_utils.replace_dict_keys(self.data.entries, mol_ret)
        self._new_molecules = {}

        for k in list(self._new_keywords.keys()):
            ret = client.add_keywords([self._new_keywords[k]])
            assert len(ret) == 1, "KeywordSet added incorrectly"
            self.data.alias_keywords[k[0]][k[1]] = ret[0]
            del self._new_keywords[k]

    def _default_parameters(self, driver, keywords, program):

        if program is None:
            if self.data.default_program is None:
                raise KeyError("No default program was set and none was provided.")
            program = self.data.default_program
        else:
            program = program.lower()

        if driver is None:
            driver = self.data.default_driver

        if keywords is None:
            if program in self.data.default_keywords:
                keywords = self.data.alias_keywords[program][self.data.default_keywords[program]]
        else:
            if (program not in self.data.alias_keywords) or (keywords not in self.data.alias_keywords[program]):
                raise KeyError("KeywordSet alias '{}' not found for program '{}'.".format(keywords, program))

            keywords = self.data.alias_keywords[program][keywords]

        return driver, keywords, program

    def set_default_program(self, program: str) -> bool:
        """
        Sets the default program.
        """

        self.data.default_program = program.lower()

    def add_keywords(self, alias: str, keyword: 'KeywordSet', default: bool=False) -> bool:
        """
        Adds an option alias to the dataset. Not that keywords are not present
        until a save call has been completed.

        Parameters
        ----------
        alias : str
            The alias of the option
        keyword : KeywordSet
            The Keywords object to use.
        default : bool
            Sets this option as the default for the program
        """

        alias = alias.lower()
        if keyword.program not in self.data.alias_keywords:
            self.data.alias_keywords[keyword.program] = {}

        if alias in self.data.alias_keywords[keyword.program]:
            raise KeyError("Alias '{}' already set for program {}.".format(alias, keyword.program))

        self._new_keywords[(keyword.program, alias)] = keyword

        if default:
            self.data.default_keywords[keyword.program] = alias
        return True

    def query(self,
              method,
              basis,
              driver=None,
              keywords=None,
              program=None,
              reaction_results=False,
              scale="kcal",
              field="return_result",
              ignore_ds_type=False):
        """
        Queries the local Portal for the requested keys and stoichiometry.

        Parameters
        ----------
        method : str
            The computational method to query on (B3LYP)
        basis : str
            The computational basis query on (6-31G)
        driver : str, optional
            Search within energy, gradient, etc computations
        keywords : str, optional
            The option token desired
        program : str, optional
            The program to query on
        stoich : str
            The given stoichiometry to compute.
        prefix : str
            A prefix given to the resulting column names.
        postfix : str
            A postfix given to the resulting column names.
        reaction_results : bool
            Toggles a search between the Mongo Pages and the Databases's reaction_results field.
        scale : str, double
            All units are based in Hartree, the default scaling is to kcal/mol.
        field : str, optional
            The result field to query on
        ignore_ds_type : bool
            Override of "ie" for "rxn" db types.


        Returns
        -------
        success : bool
            Returns True if the requested query was successful or not.

        Notes
        -----


        Examples
        --------

        ds.query("B3LYP", "aug-cc-pVDZ", stoich="cp", prefix="cp-")

        """

        driver, keywords, program = self._default_parameters(driver, keywords, program)

        if not reaction_results and (self.client is None):
            raise AttributeError("DataBase: FractalClient was not set.")

        query_keys = {
            "method": method.lower(),
            "basis": basis.lower(),
            "driver": driver.lower(),
            "keywords": keywords,
            "program": program.lower(),
        }
        # # If reaction results
        if reaction_results:
            tmp_idx = pd.Series(index=self.df.index)
            for rxn in self.data.reactions:
                try:
                    tmp_idx.loc[rxn.name] = rxn.reaction_results[stoich][method]
                except KeyError:
                    pass

            # Convert to numeric
            tmp_idx = pd.to_numeric(tmp_idx, errors='ignore')
            tmp_idx *= constants.get_scale(scale)

            self.df[prefix + method + postfix] = tmp_idx
            return True

        # if self.data.ds_type.lower() == "ie":
        #     _ie_helper(..)

        if (not ignore_ds_type) and (self.data.ds_type.lower() == "ie"):
            monomer_stoich = ''.join([x for x in stoich if not x.isdigit()]) + '1'
            tmp_idx_complex = self._unroll_query(query_keys, stoich, field=field)
            tmp_idx_monomers = self._unroll_query(query_keys, monomer_stoich, field=field)

            # Combine
            tmp_idx = tmp_idx_complex - tmp_idx_monomers

        else:
            tmp_idx = self._unroll_query(query_keys, stoich, field=field)
        tmp_idx.columns = [prefix + method + '/' + basis + postfix for _ in tmp_idx.columns]

        # scale
        tmp_idx = tmp_idx.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        tmp_idx[tmp_idx.select_dtypes(include=['number']).columns] *= constants.get_scale(scale)

        # Apply to df
        self.df[tmp_idx.columns] = tmp_idx

        return True

    def compute(self, method, basis, driver=None, keywords=None, program=None, stoich="default", ignore_ds_type=False):
        """Executes a computational method for all reactions in the Dataset.
        Previously completed computations are not repeated.

        Parameters
        ----------
        method : str
            The computational method to compute (B3LYP)
        basis : str
            The computational basis to compute (6-31G)
        driver : str, optional
            The type of computation to run (energy, gradient, etc)
        stoich : str, optional
            The stoichiometry of the requested compute (cp/nocp/etc)
        keywords : str, optional
            The keyword alias for the requested compute
        program : str, optional
            The underlying QC program
        ignore_ds_type : bool, optional
            Optionally only compute the "default" geometry

        Returns
        -------
        ret : dict
            A dictionary of the keys for all requested computations
        """
        self._check_state()

        if self.client is None:
            raise AttributeError("DataBase: Compute: Client was not set.")

        driver, keywords, program = self._default_parameters(driver, keywords, program)

        # Figure out molecules that we need
        if (not ignore_ds_type) and (self.data.ds_type.lower() == "ie"):
            monomer_stoich = ''.join([x for x in stoich if not x.isdigit()]) + '1'
            tmp_monomer = self.rxn_index[self.rxn_index["stoichiometry"] == monomer_stoich].copy()
            tmp_complex = self.rxn_index[self.rxn_index["stoichiometry"] == stoich].copy()
            tmp_idx = pd.concat((tmp_monomer, tmp_complex), axis=0)
        else:
            tmp_idx = self.rxn_index[self.rxn_index["stoichiometry"] == stoich].copy()

        tmp_idx = tmp_idx.reset_index(drop=True)

        # There could be duplicates so take the unique and save the map
        umols, uidx = np.unique(tmp_idx["molecule"], return_index=True)

        complete_values = self.client.get_results(
            molecule=list(umols),
            driver=driver,
            keywords=keywords,
            program=program,
            method=method,
            basis=basis,
            projection={"molecule": True})

        complete_mols = np.array([x["molecule"] for x in complete_values])
        umols = np.setdiff1d(umols, complete_mols)
        compute_list = list(umols)

        ret = self.client.add_compute(program, method.lower(), basis.lower(), driver, keywords, compute_list)

        return ret

    def get_index(self):
        """
        Returns the current index of the database.

        Returns
        -------
        ret : list of str
            The names of all reactions in the database
        """
        return [x.name for x in self.data.entries]

    # Statistical quantities
    def statistics(self, stype, value, bench="Benchmark"):
        """Summary

        Parameters
        ----------
        stype : str
            The type of statistic in question
        value : str
            The method string to compare
        bench : str, optional
            The benchmark method for the comparison

        Returns
        -------
        ret : pd.DataFrame, pd.Series, float
            Returns a DataFrame, Series, or float with the requested statistics depending on input.
        """
        return statistics.wrap_statistics(stype, self.df, value, bench)

    # Getters
    def __getitem__(self, args):
        """A wrapped to the underlying pd.DataFrame to access columnar data

        Parameters
        ----------
        args : str
            The column to access

        Returns
        -------
        ret : pd.Series, pd.DataFrame
            A view of the underlying dataframe data
        """
        return self.df[args]


register_collection(Dataset)
