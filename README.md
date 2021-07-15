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
    sbt "run -r http://localhost:18080/api/v1 -a local-1624798402391 -t 2.0"



