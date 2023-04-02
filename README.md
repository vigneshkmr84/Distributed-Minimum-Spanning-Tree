# DC-Minimum-Spanning-Tree
Minimum Spanning Tree in Distributed Computing

Team Members: Trent Haines(tsh160230), Kamalesh Palanisamy(kxp210004), Vignesh Thirunavukkarasu(vxt200003)

Compile and Run instructions:

1. First, download the zip file attached in the submission. Extract this zip file.
2. Connect through SSH to the UTD Servers, for example, csgrads1.utdallas.edu, and upload the entire contents of the zip file.
3. Change directory into the new folder uploaded, DC-Minimum-Spanning-Tree by entering the command: cd DC-Minimum-Spanning-Tree
4. Run the setup shell script in order to install compilation dependencies such as Maven by entering the command: ./setup.sh
5. Compile the source files using Maven by entering the command: mvn compile
6. Package the source files using Maven by entering the command: mvn package
7. Modify the ./launcher.sh and ./cleanup.sh to reflect your local netid, project, and config paths. If you are using a mac, you would need to run ./launcher-mac.sh from your mac terminal.
8. Edit the config.txt file to modify the test input to whatever input you would like to test.
9. Run the program by entering the command: ./launcher.sh
The program will now run and display the output for the Distributed Minimum Spanning Tree Program.
10. Once you are satisfied, clean up the program by entering the command: ./cleanup.sh
11. Repeat steps 8-10 as desired.

In case you are not interested in running maven, we have added the mst.jar file to the repository. Create a directory called target inside the repository and add the jar file to it. You can then follow steps 7-11 from above.