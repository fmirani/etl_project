MYDIR=$(HOME)/etl
DATADIR=$(MYDIR)/data
LOGSDIR=$(DATADIR)/logs
CONFDIR=$(MYDIR)/config

CURRDIR=$(shell pwd)
SRCDIR=$(CURRDIR)/etl


clean:
	rm -rf $(MYDIR)
	rm $(SRCDIR)/data/logs/etl.log
	@echo "\nEnter 'make install' to install directory structure\n"


install:
	[ -d $(MYDIR) ] || mkdir -p $(MYDIR)
	[ -d $(DATADIR) ] || mkdir -p $(DATADIR)
	[ -d $(LOGSDIR) ] || mkdir -p $(LOGSDIR)
	[ -d $(CONFDIR) ] || mkdir -p $(CONFDIR)

	touch $(SRCDIR)/data/logs/etl.log

	mv $(SRCDIR)/config/example_config.yaml $(SRCDIR)/config/config.yaml

	ln -s $(SRCDIR)/config/config.yaml $(CONFDIR)/config.yaml
	ln -s $(SRCDIR)/data/logs/etl.log $(LOGSDIR)/etl.log

	@echo "\n\nDirectory structure installation complete."
	@echo "Please make sure you have source data files in $(MYDIR)"
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
	@echo "\nEnter make 'full_run' to execute full ETL cycle\n"


full_run:
	cp $(SRCDIR)/main.py .
	python main.py

	rm main.py