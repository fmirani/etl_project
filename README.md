
This is a simple ETL project that extracts input data (watched history) from two sources: YouTube and Netflix, transforms it and loads in a database. The choice of the data sources was based on my personal media consumption habbits, but that made the 'E' part of the ETL next to useless as neither YouTube nor Netflix allows (anymore, I must add) to fetch our watched history through API calls. The 'E' part therefore is (sort-of) simulated, and the input data files are retrived and copied manually.

The pipeline
-> Extract source data (YouTube and Netflix watched videos history)
    - YouTube: .html file downloaded from Google account
    - Netflix: .csv file download from Netflix account
-> Transform data
    - Receive extracted files
    - Parse the data through both file formats
    - Set it up in a single table (Pandas DataFrame)
-> Load data
    - Go through the table
    - See if any items in the current table already exists in the database
    - See if some information could be completed before loading the data
