CXX ?= c++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -pedantic

all: metrics

metrics: metrics.cpp
	$(CXX) $(CXXFLAGS) -o metrics metrics.cpp

clean:
	rm -f metrics

.PHONY: all clean
