prefix_patterns = {
    re.compile(r'^Entity:\s+[^\s]+\s+Mandatory'): 'Entity-Mandatory',
    re.compile(r'^Entity:\s+[^\s]+\s+[(]orphaned[)].\s+Mandatory'): 'Entity-Mandatory-Orphaned-1',
    re.compile(r'^Entity:\s+[^\s]+\s+[(]orphaned[)][]].\s+Mandatory'): 'Entity-Mandatory-Orphaned-2',
    re.compile(r'^Entity\s+[^\s]+\s+with name.*is (selling|buying)'): 'EntityIllegalComm',
    re.compile(r'^Entity:\s+[^\s]+\s+delete[.]\s+Mandatory'): 'Entity-Delete',
    re.compile(r'^Applying \d+ stitching'): 'Stitching',
    re.compile(r'^Applying \d+ pre-stitching'): 'PreStitching',
    re.compile(r'^Applying \d+ post-stitching'): 'PostStitching',
    re.compile(r'^Could not update .* capacities for entity '): 'Could not update',
}
