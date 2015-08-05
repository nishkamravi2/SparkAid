if [ ! -d "build" ]; then
	mkdir build
fi
javac -d build @build.txt
cd build
jar cf sparkaid.jar .
java -classpath sparkaid.jar ConfigurationConsole "$@"
