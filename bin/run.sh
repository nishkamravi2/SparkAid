if [ ! -d "build" ]; then
  ./build.sh
fi

java -classpath sparkaid.jar console.src.main.ConfigurationConsole "$@"
python ../optimizer/src/main/main.py
rm tmp-code-file-path.txt