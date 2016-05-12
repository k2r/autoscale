To compile the scheduler without storm dependencies:

mvn clean

mvn assembly:assembly

To install the new scheduler in the installed Storm (for Windows) release:

cp target/scheduler-0.0.1-SNAPSHOT-jar-with-dependencies.jar %STORM%/lib

To install the new scheduler in the installed Storm (for Unix systems) release:

cp target/scheduler-0.0.1-SNAPSHOT-jar-with-dependencies.jar $STORM/lib