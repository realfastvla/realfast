#!/usr/bin/perl
#
# Single-point-control wrapper for realfast software package.
#
# Starts supervisord and then exits so there is no persistent job.
#



# SET DEFAULTS AND PARSE COMMAND-LINE ARGS

$rf_setup         = '/home/cbe-master/realfast/soft/realfast/scripts/rf_default.sh';     
                                            # default REALFAST setup file (environment vars sh file)
$workers_per_node = 1;                      # number of workers per node
chomp($workdir    = `pwd`);                 # working directory
$supervisord_conf = '';                     # Default set to $workdir/supervisord.conf below.
$redisnode = 'cbe-node-01';

if (!-e $rf_setup){
    throw_error("Couldn't find setup file.");
    exit 1;
}

#$arg_string = parse_args();



# Set environment variables (call .sh file)

realfast_setup($rf_setup);
$workdir = $ENV{'WORKDIR'};
if ($supervisord_conf eq ''){                # default conf file
    $supervisord_conf = "$workdir/supervisord.conf";
}



# Go to workdir; do error checking.

chdir $workdir;
print STDERR "
----------------------------------
        STARTING REALFAST

Will use working directory $workdir
----------------------------------

";

system "touch temp.garbage";
if (!-e "temp.garbage"){
    throw_error("You don't have write permission in workdir $workdir");
    exit 0;
}

if (-e "$supervisord_conf"){
    print "\n* * * WARNING:\n\tConf file $supervisord_conf already exists. Overwrite?\n\tCtrl+C to exit, Enter to continue.";
    $dummy = <STDIN>;
    print "\n";
}



# Determine which CBE control nodes are available

open(CBE_AVAIL, "grep '#' /opt/cbe-local/etc/sysconfig/wcbe_lfx_daily.conf | awk '{print substr(\$2,0,11)}' | sort | uniq |");
chomp(@cbe_nodes = <CBE_AVAIL>);
close(CBE_AVAIL);
if ($#cbe_nodes < 0){
    throw_error("Found no available CBE nodes in /opt/cbe-local/etc/sysconfig/wcbe_lfx_daily.conf");
    exit 1;
}



# Set up, run Supervisord.

print_conf("$supervisord_conf");
system "supervisord"; #!!! NEED TO CHECK FOR ERROR STATUS RETURN

# Tell user how to monitor things and exit.
print "\n----------------------------------
-     REALFAST HAS STARTED.      -
----------------------------------
Current supervisor daemon status:\n";
system "supervisorctl status";
print "----------------------------------\n";
print "\nNote: supervisord conf file is $supervisord_conf";
print "\n\nYou can update supervisord by setting new environment variables maybe?\n\n";

exit 0;




# - - - - - - - - - - - 
#     SUB-FUNCTIONS
# - - - - - - - - - - - 

# Print supervisord conf file
sub print_conf(){
    my $conf_file = shift @_;
    open (CONF, ">$conf_file");
    print CONF '; Sample supervisor config file.
;
; For more information on the config file, please see:
; http://supervisord.org/configuration.html
;
; Notes:
;  - Shell expansion ("~" or "$HOME") is not supported.  Environment
;    variables can be expanded using this syntax: "%(ENV_HOME)s".
;  - Comments must have a leading space: "a=b ;comment" not "a=b;comment".

[unix_http_server]
file=/home/cbe-master/realfast/soft/supervisor.sock   ; (the path to the socket file)    
;chmod=0700                 ; socket file mode (default 0700)
;chown=nobody:nogroup       ; socket file uid:gid owner
;username=user              ; (default is no username (open server))
;password=123               ; (default is no password (open server))

;[inet_http_server]         ; inet (TCP) server disabled by default
;port=127.0.0.1:9001        ; (ip_address:port specifier, *:port for all iface)
;username=user              ; (default is no username (open server))
;password=123               ; (default is no password (open server))

[supervisord]
logfile=/home/cbe-master/realfast/soft/supervisord.log ; (main log file;default $CWD/supervisord.log)
logfile_maxbytes=50MB        ; (max main logfile bytes b4 rotation;default 50MB)
logfile_backups=10           ; (num of main logfile rotation backups;default 10)
loglevel=info                ; (log level;default info; others: debug,warn,trace)
pidfile=/home/cbe-master/realfast/soft/supervisord.pid ; (supervisord pidfile;default supervisord.pid)
nodaemon=false               ; (start in foreground if true;default false)
minfds=1024                  ; (min. avail startup file descriptors;default 1024)
minprocs=200                 ; (min. avail process descriptors;default 200)
;umask=022                   ; (process file creation umask;default 022)
;user=chrism                 ; (default is current user, required if root)
;identifier=supervisor       ; (supervisord identifier, default is \'supervisor\')
;directory=/tmp              ; (default is not to cd during start)
;nocleanup=true              ; (don\'t clean up tempfiles at start;default false)
;childlogdir=/tmp            ; (\'AUTO\' child log dir, default $TEMP)
';
#!!! ADD ENVIRONMENT VARS HERE. I'm still not sure why the perl-set env-vars aren't being passed to supervisord; need to check that.
print ';environment=KEY="value"     ; (key value pairs to add to environment)
;strip_ansi=false            ; (strip ansi escape codes in logs; def. false)

; the below section must remain in the config file for RPC
; (supervisorctl/web interface) to work, additional interfaces may be
; added by defining them in separate rpcinterface: sections
[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix:///home/cbe-master/realfast/soft/supervisor.sock ; use a unix:// URL  for a unix socket
;serverurl=http://127.0.0.1:9001 ; use an http:// url to specify an inet socket
;username=chris              ; should be same as http_username if set
;password=123                ; should be same as http_password if set
;prompt=mysupervisor         ; cmd line prompt (default "supervisor")
;history_file=~/.sc_history  ; use readline history if available

; The below sample program section shows all possible program subsection values,
; create one or more \'real\' program: sections to be able to control them under
; supervisor.

[program:redis]
command=/users/claw/code/redis-stable/src/redis-server /users/claw/code/realfast/conf/redis_cbe.conf
priority=1    ; set up db first
redirect_stderr=True

[program:queue]
command=queue_monitor
priority=4
redirect_stderr=True
stdout_logfile=/home/cbe-master/realfast/soft/realfast.log       ; stdout log path, NONE for none; default AUTO
stdout_logfile_backups=5     ; # of stdout logfile backups (default 10)

;[program:mcaf]
;command=nice -n;%(ENV_MCAF_MON_NICE)s mcaf_monitor.py %(ENV_MCAF_INPUTS)s
;priority=5     ; watch for data last
;redirect_stderr=True
;stdout_logfile=/home/cbe-master/realfast/soft/realfast.log       ; stdout log path, NONE for none; default AUTO
';

    foreach $node (@cbe_nodes){
	for ($i=1;$i<=$workers_per_node;$i++){
	    $nodenum = substr($node,-2,2);
	    print CONF "
[program:work$nodenum-$i]
command=ssh -t $node rq worker default -u redis://$redisnode   ; -t option keeps connection and allows supervisor to (re)start, etc.
priority=3     ; start worker after db
redirect_stderr=True
autostart=False
"
	}
    }
    close CONF;
}


sub parse_args(){
    # Check for bad argv's 

    # Read input arguments into one long string (!!!minus setup
    # specification, which we want to read into $rf_setup. Also want to read workdir if it's specified; will write files there).
    $arg_string = "@ARGV";

#!H    return "@mcaf_args";
}


# Print a semi-formatted error message;
sub throw_error(){
    my $e = shift @_;
    print "\n\tERROR:\n\t";
    
    my @es = split(/\s+/,$e);
    $i=1;
    foreach $e (@es){
	print "$e ";
	if ($i == 10){
	    print "\n\t";
	    $i=0;
	}
	$i++;
    }

    print "\n\n";
}


# Read setup variables in as environment vars.
sub realfast_setup(){
    my $setup_file = shift @_;
    
    open(SETUP, "$setup_file");
    while(<SETUP>){
	chomp;
	if (substr($_,0,1) eq '#' || $_ eq ''){next;}
	my @arr = split(/\s+/,$_);
	my ($var,$val) = split('=',$arr[-1]);
	$val =~ s/\"//g;
	$val =~ s/\'//g;	
	$ENV{$var} = $val;
    }
}
