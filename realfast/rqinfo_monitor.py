import subprocess, time, click, sys, logging
logging.basicConfig(format="%(asctime)-15s %(levelname)8s %(message)s", level=logging.INFO)

@click.command()
def monitor():
    """ Runs loop to check 'rq info', but only prints when changes to queue occur.
    """

    output0 = ['']
    logging.info('Monitoring rqinfo...')
    while 1:
        try:
            output = subprocess.check_output(['rq', 'info']).split('\n\n')
            if output[:-1] != output0[:-1]:   # if new, excluding time stamp
                logging.info('\n\n'.join(output))   # print out new status
                output0 = output   # update current status
            time.sleep(2)
        except KeyboardInterrupt:
            logging.info('Escaping...')
            break

        sys.stdout.flush()  # get it to print frequently
