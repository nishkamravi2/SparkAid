if [ ! -d "build" ]; then
	mkdir build
fi
javac -d build @build.txt
cd build
java ConfigurationConsole "$@"
