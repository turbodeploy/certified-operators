#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>

/**
 * Executes the program
 */
static void exec_program(char **args) {
	pid_t child = fork();
	if (child == -1) {
		perror("Error forking child process");
		exit (1);
	} else if (child == 0) {
		// Child
		execvp(args[0], (char* const*)args);
		exit(2);
	} else {
		// Parent
		int status = 0;
		do {
			pid_t w = waitpid(child, &status, WUNTRACED | WCONTINUED);
			if (w == -1) {
				perror("waitpid");
				exit(EXIT_FAILURE);
			}
		} while (!WIFEXITED(status) && !WIFSIGNALED(status));
	}
}

/**
 * Displays the IP address
 */
void display_ip() {
	char *args[5];
	args[0] = (char *)"/usr/sbin/ip";
	args[1] = (char *)"route";
	args[2] = (char *)"get";
	args[3] = (char *)"1";
	args[4] = NULL;
	exec_program(args);
}

/**
 * Invokes the network editor
 */
void edit_network() {
	char *args[3];
	args[0] = (char *)"/usr/bin/nmtui";
	args[1] = (char *)"edit";
	args[2] = NULL;
	exec_program(args);
}

int main (int argc, char** argv) {
	display_ip();
	char c = ' ';
	do {
		printf("Do you need to edit the connection[y/n]: ");
		c = getchar();
    } while (c != 'y' && c != 'Y' && c != 'n' && c != 'N');

    // We agreed to edit the network
	if (c == 'y' || c == 'Y') {
		edit_network();
	}

	return 0;
}
