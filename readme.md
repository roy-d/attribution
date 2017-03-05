#Attribution Application

run: ```sbt "run-main lab.AttributionApp"```

test: ```sbt clean test```

output dir: ```data/output```

#CI
[![Build Status](https://travis-ci.org/roy-d/attribution.svg?branch=master)](https://travis-ci.org/roy-d/attribution)

#Working assumptions:
* Events can be de-duped prior to attribution process without any impact to results.
* For an Advertiser and User, if there is an Impression at time `t`, then all Events for that Advertiser and User after
time `t` be counted as Attributed-Events.
