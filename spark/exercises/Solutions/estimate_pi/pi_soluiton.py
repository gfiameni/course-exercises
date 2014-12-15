# Spark can also be used for compute-intensive tasks.
# This code estimates π by "throwing darts" at a circle.
# We pick random points in the unit square ((0, 0) to (1,1))
# and see how many fall in the unit circle.
# The fraction should be π / 4, so we use this to get our estimate.

NUM_SAMPLES = 100

def sample(p):
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else 0

count = spark.parallelize(xrange(0, NUM_SAMPLES)).map(sample) \
             .reduce(lambda a, b: a + b)

print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)