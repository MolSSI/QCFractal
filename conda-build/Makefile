# Makefile for building conda packages
BUILDDIR      = /tmp/qca_conda_build
CFUSER        = qcarchive
CFLABEL       = next

.PHONY: qcfractal qcportal qcfractalcompute qcarchivetesting clean all

all: qcfractal qcfractalcompute qcportal qcarchivetesting

clean:
	rm -Rf $(BUILDDIR)

qcportal:
	conda mambabuild -c conda-forge --output-folder $(BUILDDIR) qcportal/

qcfractalcompute: qcportal
	conda mambabuild -c conda-forge --output-folder $(BUILDDIR) qcfractalcompute/

qcfractal: qcfractalcompute
	conda mambabuild -c conda-forge --output-folder $(BUILDDIR) qcfractal/

qcarchivetesting: qcfractal
	conda mambabuild -c conda-forge --output-folder $(BUILDDIR) qcarchivetesting/

upload: 
	anaconda upload $(BUILDDIR)/noarch/qc*-py_0.tar.bz2 --user $(CFUSER) --label $(CFLABEL)
