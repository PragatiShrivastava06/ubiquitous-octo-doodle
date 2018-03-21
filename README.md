Following instructions and commands are for Mac. Pls let me know if you wish to run on Windows as there might be a few additional steps.

1)	This application requires Spark version 2.3.0, Python 2.7.10 and Cement-2.10.2.
    i) pip install pyspark - http://spark.apache.org/downloads.html
    ii) pip install Cement - http://builtoncement.com/2.10/dev/installation.html?highlight=install

2)	Download the source code from public repository on GitHub - https://github.com/PragatiShrivastava06/ubiquitous-octo-doodle
3)	Put logparser.py, test-access.log (input) file under same folder (say, project folder).
4)	Through the command line navigate to above project folder
5)	Type ls -l on command prompt, you should see these 2 files - logparser.py and test-access.log
6)	Steps to run the application, on the command line type the following 4 commands:
	i) python logparser.py ––help
	      Above command will give you information about sub commands to run the application
    ii) python logparser.py ps
		  Above command will plot a histogram of all HTTP statuses
    iii) python logparser.py rt
		  Above command will print the 10 largest request times
    iv) python logparser.py mm
          Above command will print the mean and median request times
Note :  python logparser.py command will prompt you to use command 6(i)

Note : For Mac User, use line 116 in logparser.py as shown below,
spark = SparkSession.builder.appName("LogParser").getOrCreate()

For Windows User, Comment line 116 and uncomment line 117 as shown below,
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("LogParser").getOrCreate()

Output:
i) python logparser.py ––help
    My Application parse access logs!
    commands:
        plot-HTTP-status (aliases: ps)
            Plots a histogram of all HTTP statuses.
        request-time (aliases: rt)
            Prints the 10 largest request times.
        retrieve-mean-median (aliases: mm)
            Prints the mean and median request times.
    optional arguments:
        -h, --help  show this help message and exit
        --debug     toggle debug output
        --quiet     suppress all output

ii) python logparser.py ps
    Plotting a histogram of all HTTP statuses.

iii) python logparser.py rt
     Printing the 10 largest request times.
        +-----------------------+
        |10 Largest Request Time|
        +-----------------------+
        |                 299.97|
        |                299.697|
        |                298.515|
        |                298.131|
        |                297.846|
        |                297.508|
        |                 296.69|
        |                291.005|
        |                290.617|
        |                288.285|
        +-----------------------+
    only showing top 10 rows

iv) python logparser.py mm

    Printing the mean and median request times.
        +------------------+
        | Mean Request Time|
        +------------------+
        |1.2384423399456854|
        +------------------+

    Median Request Time = 0.001