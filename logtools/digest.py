#!/usr/bin/env python

import sys
import re
import os

# This script analyzes log data and organizes it into "digests" that are then
# summarized in a summary file.
#
# A digest is identified by a "prefix" of the "primary" line of a log message.
# A log message may consume multiple lines, and primary lines are identified
# by containing " INFO " or " WARN " or " ERROR ". (That could be tightened up.)
# All lines following a primary line that are not themselves primary lines
# are considered part of the same log message, and are included in the
# digest.
#
# The default prefix is the first 20 characters of the meat of the primary
# line - specifically, anything following the first colon following the
# log level (with leading spaces stripped).
#
# A prefix_patterns map of regex to string can be used to assign
# digests to different patterns based on regex matching.
#
# The digest captures: the prefix, the number of lines in the log
# message, and the total number of characters (including text
# preceding the prefix).
#
# Summary output includes, for each discovered prefixes, the number of
# digests with that prefix, and the sums of the their line and character
# counts. Each count is also shown as a percentage of total, and they
# are listed in reverse numeric order of character count.
#
# The purpose of this is to provide information that can help in
# locating logging hot-spots - code points that produce a larege amount
# of logging data. Default prefixes can often be used in code search
# to locate the code producing the logging activity.
#                                                     
# N.B. This script was written by a novice python developer, using
# a monkey-see-monkey-do approach with lots of googling.


# regex -> prefix, tried in order of appearance before extracting default prefix
prefix_patterns = {}
# definitions for a specific log file name can be defined externally
# this probably ought to be done with modules, but it's currently beyond me
try:
    dir=os.path.dirname(os.path.realpath(__file__))
    execfile(dir+"/"+sys.argv[1]+"_module.py")
except:
    pass

def main(name):
    input = open(name+".log", "r")
    output = open(name+".digest", "w")
    summarizer = Summarizer()
    while True:
        digest = Digest(summarizer, prefix_patterns).load(input)
        if digest is not None:
            output.write(str(digest))
        else:
            break
    summaryOutput = open(name+".summary", "w")
    summaryOutput.write(summarizer.getResults())

class Digest:
    logre = r'\s(INFO|WARN|ERROR)[^:]*:\s*(.*)'
    leftover = None

    def __init__(self, summarizer, prefix_patterns):
        self.summarizer = summarizer
        self.prefix_patterns = prefix_patterns
        self.complete = False
        self.line1 = None
        self.prefix = None
        self.lines = 0
        self.chars = 0

    def load(self, infile):
        if Digest.leftover is not None:
            self.consume(Digest.leftover)
            Digest.leftover = None
        for line in infile:
            self.consume(line)
            if self.complete:
                break
        if not self.complete:
            if self.line1 is not None:
                self.complete = True
            else:
                return None
        self.summarizer.post(self)
        return self
    
    def consume(self, line):
        match = re.search(Digest.logre, line)
        if match:
            if self.line1 is None:
                self.line1 = match.group(2).rstrip()
                self.prefix = self.getPrefix(self.line1)
                self.lines = 1
                self.chars = len(line)
            else:
                Digest.leftover = line
                self.complete = True
        elif self.line1 is not None:
            self.lines = self.lines + 1
            self.chars = self.chars + len(line)

    def getPrefix(self, line):
        for regex,p in self.prefix_patterns.items():
            if re.match(regex, line) is not None:
                return p
        return line[0:20]
        
    def __str__(self):
        return '"%s",%d,"%s"\n' % (self.prefix, self.lines, self.line1)

class Summarizer:
    def __init__(self):
        self.summaries = {}

    def post(self, digest):
        summary = self.getSummary(digest.prefix)
        summary.update(digest)

    def getSummary(self, prefix):
        summary = self.summaries.get(prefix, None)
        if summary is None:
            summary = Summary(prefix)
            self.summaries[prefix] = summary
        return summary

    def getResults(self):
        sorted = self.summaries.values()
        sorted.sort(key=lambda s: s.chars, reverse=True)
        totals = self.getTotals()
        sorted.insert(0, totals)
        return "\n".join([v.summarize(totals) for v in sorted])

    def getTotals(self):
        digests = 0
        lines = 0
        chars = 0
        for summary in self.summaries.values():
            digests = digests + summary.digests
            lines = lines + summary.lines
            chars = chars + summary.chars
        return Summary("[Totals]", digests, lines, chars)

class Summary:
    def __init__(self, prefix, digests=0, lines=0, chars=0):
        self.prefix = prefix
        self.digests = digests
        self.lines = lines
        self.chars = chars

    def update(self, digest):
        self.digests = self.digests + 1
        self.lines = self.lines + digest.lines
        self.chars = self.chars + digest.chars

    def summarize(self, totals):
        return "'%s': %d digests (%.1f%%), %d lines (%.1f%%), %d chars (%.1f%%)" \
            % (self.prefix, \
               self.digests, 100.0*self.digests/(totals.digests or 1),
               self.lines, 100.0*self.lines/(totals.lines or 1),
               self.chars, 100.0*self.chars/(totals.chars or 1))

main(sys.argv[1])
