#Attribution Application

run: ```sbt "run-main lab.AttributionApp"```

test: ```sbt clean test```

output dir: ```data/output```

#CI
[![Build Status](https://travis-ci.org/roy-d/attribution.svg?branch=master)](https://travis-ci.org/roy-d/attribution)

#Outstanding questions:
* Can events be de-duped before running attribution? Or, should de-duping happen only prior to the counting of events?
* For an Advertiser and User, if there is an Impression at time t, then can all events for that Advertiser and User after
time t be counted as attributed events? or, should only the first event after time t qualifies as attributed event?
