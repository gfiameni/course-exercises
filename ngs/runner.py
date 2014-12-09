
# Load class for mapreduce
from newjob import MRcoverage
#from job import MRcoverage
from mrjob.util import log_to_stream

if __name__ == '__main__':

    # Create object
    mrjob = MRcoverage(args=[   \
        '-r', 'inline',
        # '-r', 'hadoop',
        # '--jobconf=mapreduce.job.maps=10',
        # '--jobconf=mapreduce.job.reduces=4'
        ])

    # Run and handle output
    with mrjob.make_runner() as runner:

        # Redirect hadoop logs to stderr
        log_to_stream("mrjob")
        # Execute the job
        runner.run()

        # Do something with stream output (e.g. file, database, etc.)
        for line in runner.stream_output():
            key, value = mrjob.parse_output_line(line)
            print key, value
