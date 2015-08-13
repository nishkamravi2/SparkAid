if [ ! -d "build" ]; then
	mkdir build
	javac -d build @build.txt
	cd build
	jar cf sparkaid.jar .
	mv sparkaid.jar ..
	cd ..
fi


