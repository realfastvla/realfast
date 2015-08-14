import sys, subprocess, click

def writeflush(dest, s):
    dest.write(s)
    dest.flush()

def get_headers(line):
    """
    Parse Supervisor message headers.
    """

    return dict([x.split(':') for x in line.split()])

def supervisor_events(stdin, stdout):
    """
    An event stream from Supervisor.
    """

    while True:

        writeflush(stdout, 'READY\n')

        line = stdin.readline()
        headers = get_headers(line)
        payload = stdin.read(int(headers['len']))
        payload = payload.split('\n')
        yield payload[0], '\n'.join(payload[1:])
        writeflush(stdout, 'RESULT 2\nOK')

email_prefix = """Heads-up! Data coming... \n\n"""

@click.command()
@click.option('--process', '-p', default='', help='Name of process to select. Default is all.')
@click.option('--select', '-s', default='', help='Select lines containing this string. Default is all.')
@click.option('--destination', '-d', default='stderr', help='Send lines to either stderr or email. Default is stderr (which is saved as log file)')
def main(process, select, destination):
    for ehead, edata in supervisor_events(sys.stdin, sys.stdout):
        ehead_parsed = get_headers(ehead)

        if process in ehead_parsed['processname']:    # select process from head
            if select in edata:     # select by string in edata
                if not len(edata):                # state event has no data. ehead becomes edata
                    edata = ehead   
                    
                # send data along
                if destination == 'stderr':
                    writeflush(sys.stderr, edata + '\n')
                elif destination == 'email':
                    subprocess.call("""echo "%s" | mailx -s 'Supervisor Event (process %s and select %s)' caseyjlaw@gmail.com""" % (email_prefix+str(edata), process, select), shell=True)

if __name__ == '__main__':
    main()
