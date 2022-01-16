HOMEDIR=$(HOME)/etl
DATADIR=$(HOMEDIR)/data
LOGSDIR=$(DATADIR)/logs
CONFDIR=$(HOMEDIR)/config

CURRDIR=$(shell pwd)
SRCDIR=$(CURRDIR)/etl
SRCLOGS=$(SRCDIR)/data/logs


clean:
	rm -rf $(HOMEDIR)/
	rm -rf $(CURRDIR)/build/
	rm -rf $(CURRDIR)/dist/
	rm -rf $(CURRDIR)/etl.egg-info/
	rm -f $(SRCDIR)/data/logs/etl.log


dir:
	[ -d $(HOMEDIR) ] || mkdir -p $(HOMEDIR)
	[ -d $(DATADIR) ] || mkdir -p $(DATADIR)
	[ -d $(LOGSDIR) ] || mkdir -p $(LOGSDIR)
	[ -d $(CONFDIR) ] || mkdir -p $(CONFDIR)
	[ -d $(SRCLOGS) ] || mkdir -p $(SRCLOGS)

	touch $(SRCLOGS)/etl.log

	cp $(SRCDIR)/data/api_key.txt $(HOMEDIR)/api_key.txt

	ln -s $(SRCDIR)/config/config.yaml $(CONFDIR)/config.yaml

	@echo "\n\nDirectory structure installation complete."
	@echo "Please make sure you have source data files in $(HOMEDIR)"
	@echo "and set source data file names in $(CONFDIR)/config.yaml"
	@echo "\n**************************************************** \n"
	@echo "Enter 'make packages' to install required Python packages\n"


packages:
ifeq ($(VIRTUAL_ENV), )
	@echo "Virtual environment is currently NOT activated"
	@echo "Please (create and) activate it before running this command"
else
	@echo "Virtual environment is currently activated"
	@echo "Installing necessary Python packages"
	pip install -r $(CURRDIR)/requirements.txt
endif
	@echo "\nEnter 'make full_run' to execute full ETL cycle\n"


build:
ifeq ($(VIRTUAL_ENV), )
	@echo "Virtual environment is currently NOT activated"
	@echo "Please (create and) activate it before running this command"
else
	@echo "Virtual environment is currently activated"
	@echo "Building an installable package for the project"
	python $(CURRDIR)/setup.py bdist_wheel
endif


install:
ifeq ($(VIRTUAL_ENV), )
	@echo "Virtual environment is currently NOT activated"
	@echo "Please (create and) activate it before running this command"
else
	@echo "Virtual environment is currently activated"
	@echo "Installing project package"
	pip install $(shell find $(CURRDIR)/dist -name 'etl*.whl')
endif


# Not intended to be used by anyone else
setup:
	cp yt_history.html $(HOMEDIR)
	cp nf_history.csv $(HOMEDIR)
	cat api_key.txt > $(HOMEDIR)/api_key.txt


full_run:
	cp $(SRCDIR)/main.py .
	python main.py

	rm main.py
