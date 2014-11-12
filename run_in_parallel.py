#!/usr/bin/env python
# Fredrik Boulund 2014
# Run a program on multiple data files on C3SE Glenn

from sys import argv, exit
from subprocess import Popen, PIPE
import os
import argparse

def parse_commandline():
    """Parse commandline.
    """

    desc="""Run in parallel on C3SE Glenn. Fredrik Boulund 2014"""

    parser = argparse.ArgumentParser(description=desc)

    slurm = parser.add_argument_group("SLURM", "Set slurm parameters.")
    slurm.add_argument("-N", type=int,
        default=1,
        help="Number of nodes [%(default)s].")
    slurm.add_argument("-p", 
        default="glenn",
        help="Slurm partition [%(default)s].")
    slurm.add_argument("-A", 
        default="SNIC2014-1-183",
        help="Slurm account [%(default)s].")
    slurm.add_argument("-t", 
        default="01:00:00",
        help="Max runtime per job [%(default)s].")

    program_parser = parser.add_argument_group("PROGRAM", "Program call in sbatch script.")
    program_parser.add_argument("--call", 
        default="",
        help="Program and arguments in a single quoted string, "+\
            "e.g. 'blat dbfile.fasta {query} -t=dnax q=prot {query}.blast8'. "+\
            "{query} is substituted for the filenames specified on "+\
            "the command line (one at a time).")
    program_parser.add_argument("query", nargs="+", metavar="FILE",
        default="",
        help="Query file(s).")


    if len(argv)<2:
        parser.print_help()
        exit()

    options = parser.parse_args()
    return options



def generate_sbatch_script(options, query_file):
    """Generate sbatch script.
    """

    call = options.call.format(query=query_file)
    
    sbatch_script = "\n".join(["#!/usr/bin/env bash",
        "#SBATCH -N {N}",
        "#SBATCH -p {p}",
        "#SBATCH -A {A}",
        "#SBATCH -t {t}",
        "{call}",
        ""]).format(N=options.N, 
            p=options.p, 
            A=options.A, 
            t=options.t, 
            call=call)
    return sbatch_script


def call_sbatch(sbatch_script):
    """Run sbatch in a subprocess.
    """

    sbatch = Popen("sbatch", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = sbatch.communicate(sbatch_script)
    if err:
        raise Exception("sbatch error: {}".format(err))



if __name__ == "__main__":
    options = parse_commandline()
    for query_file in options.query:
        sbatch_script = generate_sbatch_script(options, query_file)
        print "Launching sbatch for '{}'".format(query_file)
        call_sbatch(sbatch_script)
