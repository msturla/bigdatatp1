#!/bin/bash --


if hash mvn 2>/dev/null; then
	cd pinkelephant
	echo "Compiling java udfs"
	mvn package
	cd ..
    else
        echo "Oops! mvn is not installed. Using already built .jar"
    fi

cp pinkelephant/target/pinkelephant-0.0.1-SNAPSHOT.jar pk.jar

echo "You can now run your pig queries!"