// make protobuf definitions of core module available in this module (needed for compilation with protoc)
compileOrder := CompileOrder.JavaThenScala
//includePaths in protobufConfig ++= (sourceDirectories in (LocalProject("core"), protobufConfig)).value
//javaSource in protobufConfig := ((sourceDirectory in Compile).value / "java")
