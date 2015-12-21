#include "Errors.h"

void socketError(char *error_msg)
{
	printf("%s: %s\n", error_msg, strerror(errno));
	exit(EXIT_FAILURE);	
}

void systemError(char *error_msg)
{
	printf("%s: %s\n", error_msg, strerror(errno));
	exit(EXIT_FAILURE);
}

void applicationError(char *error_msg)
{
	printf("%s", error_msg);
	exit(0);
}

void parseError(char *error_msg)
{
	printf("Parse Error: %s\n", error_msg);
	exit(0);
}

char parseChar(char *message)
{
  	char character;
  	memcpy(&character, message, 1);
  	return character;
}
	
short parseShort(char *message)
{
  	short value;
//aisha	printf("PPPPPPPPPPPPPPPP parseShort got this message: \"%s\"\n", message);
  	memcpy(&value, message, 2);
//aisha	printf("\tPPPPPPPPP parseShort keys value is: \"%hd\"\n", value);
//aisha	printf("\tPPPPPPPPP parseShort keys value to HBO: \"%hd\"\n", (short)(ntohs(value)));
  	return (short)(ntohs(value));
}

int parseInt(char *message)	
{
	int value;
	memcpy(&value, message, 4);
	return (int)(ntohl(value));
}

