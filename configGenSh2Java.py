def convertElement2Line(key, value, type):
    retLine = "    public static final {} {} = ".format(type, key)
    if type == "String":
        return retLine + "\"{}\";".format(value)
    elif type == "int":
        return retLine + "{};".format(value)
    else:
        assert 1 == 0

def writeConf(confStr):
    confPathList = ["./src/main/java/com/madamaya/l3stream/conf/L3Config.java", "./l3stream-genealog/src/main/java/io/palyvos/provenance/l3stream/conf/L3conf.java"]
    for confPath in confPathList:
        with open(confPath) as f:
            lines = f.readlines()

        for idx in range(len(lines)):
            if lines[idx] == "//WRITE HERE//\n":
                lines = lines[:(idx+1)] + confStr + lines[(idx+1):]

        with open(confPath, "w") as w:
            w.writelines(lines)

if __name__ == "__main__":
    # Read ./bin/config.sh
    with open("./bin/config.sh") as f:
        lines = f.readlines()

    confMap = {}
    for line in lines:
        line = line.replace("\n", "").replace("\"","")
        if line == 0 or (len(line) > 0 and line[0] == "#"):
            continue
        elements = line.split("=")
        if len(elements) == 2:
            confMap[elements[0]] = elements[1]

    javaConfStr = [
        convertElement2Line("BIN_DIR", confMap["BIN_DIR"].replace("${L3_HOME}", confMap["L3_HOME"]), "String") + "\n",
        convertElement2Line("REDIS_IP", confMap["redisIP"], "String") + "\n",
        convertElement2Line("REDIS_PORT", confMap["redisPort"], "int") + "\n",
        convertElement2Line("FLINK_IP", confMap["flinkIP"], "String") + "\n",
        convertElement2Line("FLINK_PORT", confMap["flinkPort"], "int") + "\n",
        convertElement2Line("BOOTSTRAP_IP_PORT", confMap["bootstrapServers"], "String") + "\n",
        convertElement2Line("PARALLELISM", confMap["parallelism"], "int") + "\n",
        convertElement2Line("CPMSERVER_IP", confMap["cpmIP"], "String") + "\n",
        convertElement2Line("CPMSERVER_PORT", confMap["cpmPort"], "int") + "\n"
    ]

    writeConf(javaConfStr)