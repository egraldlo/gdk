################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
src/mserver5.c \
src/gdk_aggr.c \
src/gdk_align.c \
src/gdk_atoms.c \
src/gdk_bat.c \
src/gdk_batop.c \
src/gdk_bbp.c \
src/gdk_calc.c \
src/gdk_delta.c \
src/gdk_group.c \
src/gdk_heap.c \
src/gdk_logger.c \
src/gdk_mapreduce.c \
src/gdk_posix.c \
src/gdk_qsort.c \
src/gdk_rangejoin.c \
src/gdk_relop.c \
src/gdk_sample.c \
src/gdk_select.c \
src/gdk_search.c \
src/gdk_setop.c \
src/gdk_ssort.c \
src/gdk_storage.c \
src/gdk_system.c \
src/gdk_tm.c \
src/gdk_utils.c \
src/gdk_value.c \
src/getopt.c \
src/getopt1.c \
src/monet_options.c \
src/monet_version.c \
src/mutils.c \
src/stream.c \



OBJS += \
src/mserver5.o \
src/gdk_aggr.o \
src/gdk_align.o \
src/gdk_atoms.o \
src/gdk_bat.o \
src/gdk_batop.o \
src/gdk_bbp.o \
src/gdk_calc.o \
src/gdk_delta.o \
src/gdk_group.o \
src/gdk_heap.o \
src/gdk_logger.o \
src/gdk_mapreduce.o \
src/gdk_posix.o \
src/gdk_qsort.o \
src/gdk_rangejoin.o \
src/gdk_relop.o \
src/gdk_sample.o \
src/gdk_select.o \
src/gdk_search.o \
src/gdk_setop.o \
src/gdk_ssort.o \
src/gdk_storage.o \
src/gdk_system.o \
src/gdk_tm.o \
src/gdk_utils.o \
src/gdk_value.o \
src/getopt.o \
src/getopt1.o \
src/monet_options.o \
src/monet_version.o \
src/mutils.o \
src/stream.o \

#C_DEPS += \
src/mserver5.d \
src/gdk_aggr.d \
src/gdk_align.d \
src/gdk_atoms.d \
src/gdk_bat.d \
src/gdk_batop.d \
src/gdk_bbp.d \
src/gdk_calc.d \
src/gdk_delta.d \
src/gdk_group.d \
src/gdk_heap.d \
src/gdk_logger.d \
src/gdk_mapreduce.d \
src/gdk_posix.d \
src/gdk_qsort.d \
src/gdk_rangejoin.d \
src/gdk_relop.d \
src/gdk_sample.d \
src/gdk_select.d \
src/gdk_search.d \
src/gdk_setop.d \
src/gdk_ssort.d \
src/gdk_storage.d \
src/gdk_system.d \
src/gdk_tm.d \
src/gdk_utils.d \
src/gdk_value.d \
src/getopt.d \
src/getopt1.d \
src/monet_options.d \
src/monet_version.d \
src/mutils.d \
src/stream.d \


# Each subdirectory must supply rules for building sources it contributes
%.o: /%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -lpthread -lbz2 -lcurl -lxml2 -lpcre -lnuma -O3 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o)" -MT"$(@:%.o)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


