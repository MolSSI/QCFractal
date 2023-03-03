for I in *.json; do python3 fix.py $I; done

for I in sp_*.json; do python3 generate_singlepoint.py $I; done
for I in opt_*.json; do python3 generate_optimization.py $I; done
for I in td_*.json; do python3 generate_torsiondrive.py $I; done
for I in go_*.json; do python3 generate_gridoptimization.py $I; done
for I in mb_*.json; do python3 generate_manybody.py $I; done
for I in rxn_*.json; do python3 generate_reaction.py $I; done
for I in neb_*.json; do python3 generate_neb.py $I; done
