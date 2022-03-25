import os
import signal
import subprocess

def get_log_dir():
    log_dir = "log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return log_dir 


def prepare_cmd(log_dir, file_name):
    cmd = f"""
        go test -run 2A > {log_dir}/{file_name}
        go test -run 2B >> {log_dir}/{file_name}
        go test -run 2C >> {log_dir}/{file_name}
        go test -run 2D >> {log_dir}/{file_name}
        if [ $(grep -o 'ok' {log_dir}/{file_name} | wc -l) -ne 4 ]; then
            echo {log_dir}/{file_name}
        fi
    """
    return cmd

def main():
    log_dir = get_log_dir()
    procs = []
    count = 100
    for i in range(count):
        procs.append(subprocess.Popen(prepare_cmd(log_dir, i), shell=True))
    try:
        for i in range(count):
            procs[i].wait()
    except KeyboardInterrupt:
        for i in range(count):
            procs[i].send_signal(signal.SIGINT)


if __name__ == "__main__":
    main()