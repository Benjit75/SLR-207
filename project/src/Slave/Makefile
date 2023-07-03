# Makefile for compiling Java files

# Java compiler
JC = javac

# Compiler flags
JFLAGS = -d out

# Source files
SOURCES := $(wildcard Slave/Signals/*.java) Slave/SlaveInterface.java /Slave/Slave.java

# Class files
CLASSES := $(SOURCES:.java=.class)

# Output directory
OUTPUT_DIR = out

# Target: all
all: $(OUTPUT_DIR) $(CLASSES)

# Rule for compiling Java files
$(OUTPUT_DIR)/%.class: src/%.java
	$(JC) $(JFLAGS) $<

# Create the output directory
$(OUTPUT_DIR):
	mkdir -p $(OUTPUT_DIR)

# Target: clean
clean:
	rm -rf $(OUTPUT_DIR)

.PHONY: all clean
