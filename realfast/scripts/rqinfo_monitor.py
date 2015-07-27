import subprocess, time, click

@click.command()
def monitor():
    """ Runs loop to check 'rq info', but only prints when changes to queue occur.
    """

    output0 = ['']
    print 'Monitoring rqinfo...'
    while 1:
        try:
            output = subprocess.check_output(['rq', 'info']).split('\n\n')
            if output[:-1] != output0[:-1]:   # if new, excluding time stamp
                print '\n\n'.join(output)   # print out new status
                output0 = output   # update current status
            time.sleep(2)
        except KeyboardInterrupt:
            print 'Escaping...'
            break
