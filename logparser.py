from cement.core.foundation import CementApp
from cement.core.controller import CementBaseController, expose
from pyspark.sql import SparkSession
from pyspark.sql import Row
import matplotlib.pyplot as plt
import unicodedata

# define an application base controller
class MyAppBaseController(CementBaseController):
    class Meta:
        label = 'base'
        description = "My Application parse access logs!"
        epilog = "This is the text at the bottom of --help."

        config_defaults = dict(
            foo='bar',
            some_other_option='my default value',
            )

        arguments = []

    @expose(hide=True, aliases=['run'])
    def default(self):
        self.app.log.info('Not enough sub commands/arguments. Run with "--help" ')

    @expose(aliases=['ps'], help="Plots a histogram of all HTTP statuses.")
    def plot_HTTP_status(self):
        self.app.log.info("Plotting a histogram of all HTTP statuses.")

        logDataset = self.command_Part_4()

        ''' creating dataframe by selecting the http status field '''
        httpStatus = logDataset.selectExpr("cast(status as int) status")

        ''' registering dataframe as temp table '''
        httpStatus.createOrReplaceTempView("statusFrame")

        ''' selecting records where http status field is populated '''
        df = spark.sql("SELECT * FROM statusFrame WHERE status is NOT NULL")
        m_list = df.selectExpr("status as status")

        ''' creating an array of http status '''
        status_arr = [int(i.status) for i in m_list.collect()]

        ''' Plotting histogram with http status '''
        plt.hist(status_arr, bins=150)

        ''' labelling x-axis '''
        plt.xlabel('HTTP status')

        ''' labelling y-axis '''
        plt.ylabel('Frequency')

        ''' giving title to the histogram '''
        plt.title(r'Histogram of HTTP Status')
        plt.grid(True)
        plt.show()

        spark.stop()

    @expose(aliases=['rt'], help="Prints the 10 largest request times.")
    def request_time(self):
        self.app.log.info("Printing the 10 largest request times.")

        logDataset = self.command_Part_4()

        '''Creating dataframe by extracting request time field and sorting it in decreasing order and printing top 10 request times'''
        logDataset.selectExpr("cast(time as float) time"). \
            orderBy("time", ascending=False). \
            withColumnRenamed("time", "10 Largest Request Time"). \
            show(10)

        spark.stop()

    @expose(aliases=['mm'], help="Prints the mean and median request times.")
    def retrieve_mean_median(self):
        self.app.log.info("Printing the mean and median request times.")

        logDataset = self.command_Part_4()

        ''' Creating dataframe by extracting request time field and sorting it in increasing order '''
        timeSorted = logDataset.selectExpr("cast(time as float) time").orderBy("time", ascending=True)

        ''' Calculating mean request time and printing it '''
        timeSorted.agg({"time": "avg"}).withColumnRenamed("avg(time)", "Mean Request Time").show()

        ''' counting number of records '''
        cnt = timeSorted.select("time").count()

        ''' assigning index and generating rdd with key value pair '''
        rdd = timeSorted.rdd.zipWithIndex()

        ''' flipping the rdd '''
        flipped = rdd.map(lambda (x, y): (y, x))

        if (cnt % 2 == 0):
            left = cnt / 2 - 1
            right = left + 1
            ''' calculating median for rdds that has even number of records'''
            x = (flipped.lookup(left)[0].time + flipped.lookup(right)[0].time) / 2
            print ("Median Request Time = %.3f" % x)
        else:
            y = flipped.lookup(cnt / 2)
            ''' calculating median for rdds that has odd number of records '''
            print ("Median Request Time = %.3f" % flipped.lookup(cnt / 2)[0].time)

        spark.stop()

    @expose(hide=True, aliases=['cmd4'], help="this command gets called by everyone but will not be exposed to user")
    def command_Part_4(self):

        global spark

        ''' Create a SparkSession (Note, the config section is only for Windows)
            Windows user comment the next line and uncomment next to next line '''
        spark = SparkSession.builder.appName("LogParser").getOrCreate()
        # spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("LogParser").getOrCreate()

        ''' Parsing the input file'''
        def mapper(line):
            fields = line.split()
            return Row(time=unicodedata.normalize('NFKD', fields[6]).encode('ascii', 'ignore'), status=fields[10])

        ''' Reading the input file '''
        lines = spark.sparkContext.textFile("test-access.log")
        logs = lines.map(mapper)

        ''' Convert rdd to a DataFrame '''
        return spark.createDataFrame(logs).cache()

class MyApp(CementApp):
    class Meta:
        label = 'example'
        base_controller = MyAppBaseController

with MyApp() as app:
    app.run()