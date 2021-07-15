# spark-jmetrics
A tool to help optimization and troubleshooting of Apache Spark jobs by analysing job metrics


## Usage: 
    Usage: java -jar spark-jmetrics_2.12-0.1.jar --rest-endpoint <url> --app-id <id> [--out-file <file>]
    
    A tool to help optimization and troubleshooting of Apache Spark jobs by analysing job metrics
    
    Options and flags:
        --help
            Display this help text.
        --rest-endpoint <url>, -r <url>
            Rest endpoint for Spark Application or History Server (Eg: http://localhost:18080/api/v1)
        --app-id <id>, -a <id>
            Spark Application Id
        --out-file <file>, -o <file>
            Output file
        --skew-threshold <ratio>, -t <ratio>
            Data skew detection threshold on Max/Avg ratio

## Demo:
    ╭─ ~/repo/spark-jmetrics  master ⇡1                                                                                          ✔  12:20:37
    ╰─ sbt "run -r http://localhost:18080/api/v1 -a local-1624798402391 -t 1.0"

    =======> Running Spark job to analyse metrics...

    [Stage Id: 12, Attempt Id: 0 ]
    Duration                  => Avg:         22 sec , Max:   2 min 29 sec
    Bytes Read                => Avg:               0, Max:               0
    Bytes Written             => Avg:               0, Max:               0
    Shuffle Bytes Read        => Avg:               0, Max:         40 sec
    Shuffle Bytes Written     => Avg:               0, Max:               0
    
    [Stage Id: 10, Attempt Id: 0 ]
    Duration                  => Avg:         38 sec , Max:   1 min 49 sec
    Bytes Read                => Avg:               0, Max:               0
    Bytes Written             => Avg:               0, Max:               0
    Shuffle Bytes Read        => Avg:               0, Max:               0
    Shuffle Bytes Written     => Avg:               0, Max:               0
    
    [Stage Id: 14, Attempt Id: 0 ]
    Duration                  => Avg:         17 sec , Max:    1 min 5 sec
    Bytes Read                => Avg:               0, Max:               0
    Bytes Written             => Avg:               0, Max:               0
    Shuffle Bytes Read        => Avg:               0, Max:         46 sec
    Shuffle Bytes Written     => Avg:               0, Max:               0



