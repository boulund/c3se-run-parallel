#!/usr/bin/env python
# Fredrik Boulund 2014
# Run pblat on multiple data files on C3SE Glenn

from sys import argv, exit
from subprocess import Popen, PIPE
from psutil import NUM_CPUS
import os
import argparse

def parse_commandline():
    """Parse commandline.
    """

    desc="""Run in parallel on C3SE Glenn. Fredrik Boulund 2014"""

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("-N", type=int,
        default=1,
        help="Number of nodes [%(default)s].")
    parser.add_argument("-p", 
        default="glenn",
        help="Slurm partition [%(default)s].")
    parser.add_argument("-A", 
        default="SNIC2014-1-183",
        help="Slurm account [%(default)s].")
    parser.add_argument("-t", 
        default="01:00:00",
        help="Max runtime per job [%(default)s].")

    program_parser = parser.add_argument_group("BLAT")
    program_parser.add_argument("--options",
        default="-out=blast8 -t=dnax -q=prot -tileSize=5 -minScore=15 -minIdentity=80",
        help="Options to send to BLAT ['%(default)s'].")
    program_parser.add_argument("--dbfile", required=True,
        default="",
        help="Filename of DBFILE.")
    program_parser.add_argument("query", nargs="+",
        default="",
        help="Files to query against the database (can be many files).")
    program_parser.add_argument("--outdir", 
        default="mappings",
        help="Outdir to put mapping results [%(default)s].")


    if len(argv)<2:
        parser.print_help()
        exit()

    options = parser.parse_args()
    return options



def generate_sbatch_script(options, query_file):
    """Generate sbatch script.

    The sbatch script copies the dbfile and query_file to $TMPDIR on the node
    and then changes the active directory to $TMPDIR before calling the mapper.
    After mapping the result file is copied back to
    $LAUNCHDIR/outdir/outfile.blast8.
    """

    dbfile = os.path.basename(options.dbfile)
    query = os.path.basename(query_file)
    output = os.path.splitext(os.path.basename(query_file))[0]
    call = "blat {dbfile} {query} {options} {output}.blast8"
    call = call.format(dbfile=dbfile, query=query, options=options.options, output=output)
    
    sbatch_script = "\n".join(["#!/usr/bin/env bash",
        "#SBATCH -N {N}",
        "#SBATCH -p {p}",
        "#SBATCH -A {A}",
        "#SBATCH -t {t}",
        "LAUNCHDIR=`pwd`",
        "cp {dbfile} $TMPDIR",
        "cp {query_file} $TMPDIR",
        "cd $TMPDIR",
        "{call}",
        "mkdir -p $LAUNCHDIR/{outdir}",
        "cp {output}.blast8 $LAUNCHDIR/{outdir}/{output}.blast8",
        ""]).format(N=options.N, 
            p=options.p, 
            A=options.A, 
            t=options.t, 
            dbfile=options.dbfile,
            query_file=query_file,
            call=call,
            output=output,
            outdir=options.outdir)
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
