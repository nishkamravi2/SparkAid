if [ ! -d "build" ]; then
	mkdir build
	javac -d build @build.txt
	cd build
	jar cf sparkaid.jar .
	mv sparkaid.jar ..
	cd ..
	cp -r build/console/src ../auto-config/console/bin		
	cp -r build/dynamicallocation/src ../auto-config/dynamicallocation/bin	
	cp -r build/core/src ../auto-config/core/bin		
	cp -r build/streaming/src ../auto-config/streaming/bin	
	cp -r build/utils/src ../utils/bin	
fi


