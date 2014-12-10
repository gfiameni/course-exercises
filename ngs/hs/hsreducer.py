""" Streaming reducer """

# Init
import sys
TAB = "\t"
last_value = None
value_count = 0

# Cycle current streaming data
for line in sys.stdin:

    # Clean input
    line = line.strip()
    value, count = line.split(TAB)

    count = int(count)
    # if this is the first iteration
    if not last_value:
        last_value = value

    # if they're the same, log it
    if value == last_value:
        value_count += count
    else:
        # state change (previous line was k=x, this line is k=y)
        result = [last_value, value_count]
        print TAB.join(str(v) for v in result)
        last_value = value
        value_count = 1

# this is to catch the final counts after all records have been received.
print TAB.join(str(v) for v in [last_value, value_count])
