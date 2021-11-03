# Research
Code for research with Dr. Raj Bhatnagar

Github URL: https://github.com/amarkacheria/Research

Branches:
Test data (23x24)	        test-data
Bank data (local)
Can be run on PC	        bank-data-laptop
Rice data (local)
Can be run on PC	        rice-data-laptop
Rice data (cluster)
Can be run on UC cluster	rice-data-cluster

The data for each of the above datasets is located in “src /resources” of the corresponding branch.

Running Instructions:
1)	Open the code in eclipse scala
2)	Switch to the branch that you want to run
3)	If running on cluster
a.	Create the output folder in hadoop 
i.	data/{folder-name}/output
ii.	{folder-name} is a variable, depending on the branch/dataset, this will be updated
b.	Add the input file to the hadoop
4)	Run the “Driver.scala” class as a Scala Application
a.	Parameters are listed at the top of the class, change them as needed
5)	If running on cluster
a.	Download the unpredicted biclusters folder named “csv”
b.	To see the predictable clusters, download folder named “same-csv”
6)	If not running on cluster, the “csv” and “same-csv” folders will be created in “src/resources”
7)	Run the “Driver2.scala” class as a Scala Application to run stage 2 of the algorithm
8)	All the output files will be in “src/resources/output” for both the stages

Output Files:
1)	concepts.txt – List of biclusters found in stage 1
2)	confusion-matrix.txt – Shows the prediction quality and the number of labels used for stage 1
3)	training-labels.txt – List of all the rows for which we needed the labels in stage 1
4)	*.csv file – Entire dataset with predictions made by stage 1
5)	trimax/confusion-matrix-trimax.txt – Matrix after running stage 2
6)	trimax/training-labels-trimax.txt – Combined list of labels which we needed in stage 1 and stage 2
7)	trimax/*.csv file – Entire data set with final predictions after stage 1 and stage 2

Trimax Algorithm

Github URL: https://github.com/amarkacheria/trimax

This code was forked from: https://github.com/mehdi-kaytoue/trimax

Use branch: development

This code was slightly modified and then it was compiled to create the “trimax.exe” file.
The “trimax.exe” file is used in the algorithm so it is included in the previous repository:
https://github.com/amarkacheria/Research

