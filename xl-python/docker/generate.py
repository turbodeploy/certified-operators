import argparse
import os
import sys
from string import Template
import glob

mylist = [f for f in glob.glob("*.txt")]

def main():
    parser = argparse.ArgumentParser(description='Generate Python protobuf and gRPC bindings.')
    parser.add_argument('input', metavar='I', help='The folder with the proto files')
    parser.add_argument('--output', '-O', help='The folder for the output. By default the output will be at $(input)/python-grpc')

    args = parser.parse_args()
    output_dir = args.output
    if output_dir is None:
        output_dir = args.input + "/python-grpc"
    files = ""
    for file in glob.glob(args.input + "/**/*.proto", recursive=True):
        # Make the files relative to the input folder.
        files += " " + file[len(args.input)+1:]
    command = Template("python3 -m grpc_tools.protoc -I$input --python_out=$output --grpc_python_out=$output $files")
    commandStr = command.substitute(input=args.input, output=output_dir, files=files)
    os.system("mkdir -p " + output_dir)
    os.system(commandStr)
    os.system("chmod -R a+w " + args.output)

if __name__ == "__main__":
    try:
        main()
    except:
        print("Unexpected error:", sys.exc_info()[0])
    exit(0)
