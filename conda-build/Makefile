# Makefile for building conda packages
BUILDDIR      = /tmp/qca_conda_build
CFUSER        = qcarchive
CFLABEL       = next

.PHONY: qcfractal qcportal qcfractalcompute clean all

all: qcfractal qcfractalcompute qcportal

clean:
	rm -Rf $(BUILDDIR)

qcportal:
	conda-build -c conda-forge --output-folder $(BUILDDIR) qcportal/

qcfractalcompute: qcportal
	conda-build -c conda-forge --output-folder $(BUILDDIR) qcfractalcompute/

qcfractal: qcfractalcompute
	conda-build -c conda-forge --output-folder $(BUILDDIR) qcfractal/

upload: 
	anaconda upload $(BUILDDIR)/noarch/qc*-py_0.tar.bz2 --user $(CFUSER) --label $(CFLABEL)
