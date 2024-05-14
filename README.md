# Big-Data-Tools-Techniques-Project-using-SQL

The data necessary for this assignment will be zipped CSV files. The .csv files have a header
describing the files’ contents. They are:
1. Clinicaltrial_2023.csv:
Every row in the dataset corresponds to an individual clinical trial and is identified 
by different variables. It's important to note that the first column contains a mixture 
of various variables separated by a delimiter, and the date columns exhibit various 
formats. Please consider these issues and ensure that the dataset is appropriately 
prepared before initiating any analysis.
(Source: ClinicalTrials.gov)
2. pharma.csv:
The file contains a small number of a publicly available list of pharmaceutical 
violations. For the purposes of this work, we are interested in the second column, 
Parent Company, which contains the name of the pharmaceutical company in 
question. 
(Source: https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals)

You should address the following questions. 
1. The number of studies in the dataset. You must ensure that you explicitly check 
distinct studies.
2. You should list all the types (as contained in the Type column) of studies in the 
dataset along with the frequencies of each type. These should be ordered from 
most frequent to least frequent.
3. The top 5 conditions (from Conditions) with their frequencies.
4. Find the 10 most common sponsors that are not pharmaceutical companies, along 
with the number of clinical trials they have sponsored. Hint: For a basic 
implementation, you can assume that the Parent Company column contains all 
possible pharmaceutical companies.
5. Plot number of completed studies for each month in 2023. You need to include your 
visualization as well as a table of all the values you have plotted for each month
