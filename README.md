# This project is cloned from LinkedIn WhereHows
[original project link](https://github.com/linkedin/WhereHows/)

## Getting Started

New to Wherehows? Check out the [Getting Started Guide][GS]

### Preparation

First, please get Play Framework (Activator) in place.
```
# Download Activator
wget https://downloads.typesafe.com/typesafe-activator/1.3.11/typesafe-activator-1.3.11-minimal.zip

# Unzip, Remove zipped folder, move play folder to $HOME
unzip -q typesafe-activator-1.3.11-minimal.zip && rm typesafe-activator-1.3.11-minimal.zip && mv activator-1.3.11-minimal $HOME/

# Add ACTIVATOR_HOME, GRADLE_HOME. Update Path to include new gradle, alias to counteract issues
echo 'export ACTIVATOR_HOME="$HOME/activator-1.3.11-minimal"' >> ~/.bashrc
source ~/.bashrc
```

You need to increase the SBT build tool max heap size for building web module
```
echo 'export SBT_OPTS="-Xms1G -Xmx1G -Xss2M"' >> ~/.bashrc
source ~/.bashrc
```

Second, please [setup the metadata repository][DB] in MySQL.
```
CREATE DATABASE wherehows
  DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci;

CREATE USER 'wherehows';
SET PASSWORD FOR 'wherehows' = PASSWORD('wherehows');
GRANT ALL ON wherehows.* TO 'wherehows'
```

Execute the [DDL files][DDL] to create the required repository tables in **wherehows** database.


### Build

1. Get the source code: ```git clone https://github.com/linkedin/WhereHows.git```
2. Put a few 3rd-party jar files to **metadata-etl/extralibs** directory. Some of these jar files may not be available in Maven Central or Artifactory. See [the download instrucitons][EXJAR] for more detail. ```cd WhereHows/metadata-etl/extralibs```
3. Go back to the **WhereHows** root directory and build all the modules: ```./gradlew build```
4. Go back to the **WhereHows** root directory and start the metadata ETL and API service: ```cd backend-service ; $ACTIVATOR_HOME/bin/activator run```
5. Go back to the **WhereHows** root directory and start the web front-end: ```cd web ; $ACTIVATOR_HOME/bin/activator run``` Then WhereHows UI is available at http://localhost:9000 by default. For example, ```$ACTIVATOR_HOME/bin/activator run -Dhttp.port=19001``` will use port 19001 to serve UI.

## Contribute

Want to contribute? Check out the [Contributors Guide][CON]

## Community

Want help? Check out the [Google Groups][LIST]


[wiki]: https://github.com/LinkedIn/Wherehows/wiki
[GS]: https://github.com/LinkedIn/Wherehows/wiki/Getting-Started
[CON]: https://github.com/LinkedIn/Wherehows/wiki/Contributing
[VM]: https://github.com/LinkedIn/Wherehows/wiki/Quick-Start-With-VM
[EXJAR]: https://github.com/LinkedIn/Wherehows/wiki/Getting-Started#download-third-party-jar-files
[DDL]: https://github.com/linkedin/WhereHows/tree/master/data-model/DDL
[DB]: https://github.com/LinkedIn/Wherehows/wiki/Getting-Started#set-up-your-database
[LIST]: https://groups.google.com/forum/#!forum/wherehows
