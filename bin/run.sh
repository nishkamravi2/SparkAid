if [ ! -d "build" ]; then
  ./build.sh
fi

java -classpath sparkaid.jar console.src.main.ConfigurationConsole "$@"
