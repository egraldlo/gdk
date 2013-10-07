//#define PRINT
//#define STOP

int monet_print(const char *str){
#ifdef PRINT
	printf("%s\n",str);
#endif
	return 0;
}

int monet_stop(){
#ifdef STOP
	getchar();
	return 0;
#endif
}

int monet_integer(const char *str,size_t i){
	printf("%s :%d\n",str,i);
	return 0;
}
