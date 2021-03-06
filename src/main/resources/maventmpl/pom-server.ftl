<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>${parent_groupId}</groupId>
        <artifactId>${parent_artifactId}</artifactId>
        <version>${parent_version}</version>
    </parent>

    <artifactId>${artifactId}</artifactId>
    
    <packaging>jar</packaging>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>${api_groupId}</groupId>
            <artifactId>${api_artifactId}</artifactId>
            <version>${api_version}</version>
        </dependency>
        <dependency>
            <groupId>com.sparkjava</groupId>
            <artifactId>spark-core</artifactId>
            <version>2.8.0</version>
        </dependency>
        <dependency>
            <groupId>org.json</groupId>
            <artifactId>json</artifactId>
            <version>20190722</version>
        </dependency>
    </dependencies>

    <profiles>
        <profile>
            <id>deploy</id>
            <build>
                <plugins>
                    <!-- build fat jar -->
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-shade-plugin</artifactId>
                        <version>3.2.1</version>
                        <executions>
                            <execution>
                                <phase>package</phase>
                                <goals>
                                    <goal>shade</goal>
                                </goals>
                        
                                <configuration>
                                    <transformers>
                                        <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
                                        <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                            <manifestEntries>
                                                <Main-Class>${classpath}.Server</Main-Class>
                                            </manifestEntries>
                                        </transformer>
                                    </transformers>
                                    
                                    <artifactSet/>
                                    
                                    <outputFile><#noparse>${project.build.directory}/${project.artifactId}.jar</#noparse></outputFile>
                                </configuration>
                            </execution>
                        </executions>
                    </plugin>
                    <plugin>
                        <artifactId>maven-antrun-plugin</artifactId>
                        <version>1.7</version>
                        <executions>
                            <execution>
                                <id>copy</id>
                                <phase>package</phase>
                                <configuration>
                                    <tasks>
                                        <copy file="<#noparse>${project.build.directory}/${project.artifactId}.jar</#noparse>" 
                                            tofile="<#noparse>${project.build.directory}/</#noparse>${jarName}" />
                                    </tasks>
                                </configuration>
                                <goals>
                                    <goal>run</goal>
                                </goals>
                            </execution>
                        </executions>
                    </plugin>
                </plugins>
            </build>
        </profile>
    </profiles>

</project>