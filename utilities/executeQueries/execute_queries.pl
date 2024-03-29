#!/usr/bin/env perl

use strict;
use warnings;
use POSIX ":sys_wait_h";


################################################################################
# The variables below are set by the config file. The name of the ##############
# config is the first argument passed on the command line.        ##############
################################################################################

my $num_simulation_ticks = -999; 
my $min_pre_execution_sleep_interval = -999; 
my $max_pre_execution_sleep_interval = -999; 
my $min_post_execution_sleep_interval = -999; 
my $max_post_execution_sleep_interval = -999; 
my $staggered_launch_window_secs = -999;
my $min_staggered_launch_window_interval = -999; 
my $max_staggered_launch_window_interval = -999; 
#my $require_query_filename_extension = -999; 
my $query_filename_extension = "NONE";
my $max_per_query_executions = -999; 
my $create_required_directories = -999;
my $debug = -999; 
my $db_name = "fooplex";
my $actually_run_query_against_db = -999; 
my $os_type = "fooplex";
my $base_dir = "fooplex";
my $config_file = "fooplex";
my $sql_run_mode = -999;
my $kill_full_filename = "fooplex";
my $output_to_db = -999;
my $output_schema = "fooplex";
my $output_table = "fooplex";
my $output_rg_cfg_table = "";

################################################################################
# Non-Configurable #############################################################
################################################################################
my $query_output_directory; 
my $query_log_directory; 
my $stored_runtime_sql_directory; 
my $exec_summary_directory; 
my $set_variable_replacement_directory; 
my %variable_replacement; # Filename -> array.
my %queries; # Type --> ID --> ['sql', 'metadata' or 'pid'].
my %desired_query_mix;
my %pids;
my $right_now = `date "+%m_%d_%H_%M_%S"`; chomp $right_now;
my $current_simulation_ticks;
my $end_simulation_ticks;
my $end_simulation_early = 0;
my %valid_types;
my $reload_tot_queries = 0;
my $nickname = $right_now;

################################################################################
# SUBROUTINES ##################################################################
################################################################################

sub check_config_file {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my $config_filename;

    my $retval = 1; 

    my_print ($current_sub,  "\nChecking command line arguements...");
    if($#ARGV < 0) {
        my_print ($current_sub, "..Fail. Config file required.\n");
        my_print ($current_sub, "Usage: $0 <config file> [run nickname]\n");
        $retval = 0;
    } 
    else {
        my_print ($current_sub, ".. Good.\nChecking Config file for existance $ARGV[0])..."); 
        if(-e $ARGV[0]) { my_print ($current_sub, "..Good.\n"; $config_file = $ARGV[0]); }
        else { my_print ($current_sub, "..Fail. Configure file ($ARGV[0]) not found.\n"; $retval = 0;)} 

        # Use the nickname if it was provided.
        if($#ARGV == 1 and length($ARGV[1]) > 0) {
            $nickname = $ARGV[1];

            # If the name ends in 'TS', replace 'TS' with the current timestamp
            if ($nickname =~ m/TS$/) {
                my $reportTS = `date "+%d_%b_%H:%M:%S"`; chomp $reportTS;
                $nickname =~ s/TS/$reportTS/;
            }
            my_print ($current_sub, "Using the provided run nickname of $nickname\n") ;
        }
    }

    if($retval == 0) {
        my_print ($current_sub, "Cannot proceed.\n") ;
        exit 1;
    }
} # check_config_file

sub my_print {
    my ($function, $message) = @_;

    print "$function: $message\n";
}

sub verify_setup {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my $config_filename;

    my $retval = 1;

    $query_output_directory              = "$base_dir/query_output/";
    $query_log_directory                 = "$base_dir/query_logs/";
    $stored_runtime_sql_directory        = "$base_dir/query_stored_runtime_sql/";
    $exec_summary_directory              = "$base_dir/query_execution_summary/";
    $set_variable_replacement_directory  = "$base_dir/variable_replacement/";


    my_print ($current_sub, "Checking Query Output directory ($query_output_directory)...");
    if(-e $query_output_directory) { my_print ($current_sub, "..Good."); }
    elsif ($create_required_directories) { my_print ($current_sub, "..Creating.");
                                           system ('mkdir ' . $query_output_directory); }
    else { my_print ($current_sub, "..Fail."); $retval = 0; } 

    my_print ($current_sub, "Checking Query Log directory ($query_log_directory)...");
    if(-e $query_log_directory) { my_print ($current_sub, "..Good."); }
    elsif ($create_required_directories) { my_print ($current_sub, "..Creating.");
                                           system ('mkdir ' . $query_log_directory); }
    else { my_print ($current_sub, "..Fail."); $retval = 0; }

    my_print ($current_sub, "Checking Stored Runtime SQL directory ($stored_runtime_sql_directory)...");
    if(-e $stored_runtime_sql_directory) { my_print ($current_sub, "..Good."); }
    elsif ($create_required_directories) { my_print ($current_sub, "..Creating.");
                                           system ('mkdir ' . $stored_runtime_sql_directory); }
    else { my_print ($current_sub, "..Fail."); $retval = 0; }

    my_print ($current_sub, "Checking Executive Summary directory ($exec_summary_directory)...");
    if(-e $exec_summary_directory) { my_print ($current_sub, "..Good."); }
    elsif ($create_required_directories) { my_print ($current_sub, "..Creating.");
                                           system ('mkdir ' . $exec_summary_directory); }
    else { my_print ($current_sub, "..Fail."); $retval = 0; }

    my_print ($current_sub, "Checking Variable Replacement directory ($set_variable_replacement_directory)...");
    if(-e $set_variable_replacement_directory) { my_print ($current_sub, "..Good."); }
    elsif ($create_required_directories) { my_print ($current_sub, "..Creating.");
                                           system ('mkdir ' . $set_variable_replacement_directory); }
    else { my_print ($current_sub, "..Fail."); $retval = 0; }

    my_print ($current_sub, "Checking absence of kill file ($kill_full_filename)...");
    if(-e $kill_full_filename) { my_print ($current_sub, "..Fail."); $retval = 0; }
    else { my_print ($current_sub, "..Good.");}


    if($retval == 0) {
        my_print ($current_sub, "Cannot proceed.");
        exit 1;
    }
    else {
        # Additional directory manipulation. Add the date to the log file directory and create the dir.
        chomp $right_now;
        $query_log_directory = $query_log_directory . $right_now . "/";
        my $make_query_log_directory = "mkdir $query_log_directory";
        system($make_query_log_directory);
    }
} # verify_setup


sub process_config_file {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my $type_counter = 1;
    my %variable_check;
    my $retval = 1;

    # Use this structure to check for missing required variables:
    $variable_check{'output_to_db'} = 0;
    $variable_check{'kill_full_filename'} = 0;
    $variable_check{'sql_run_mode'} = 0;
    $variable_check{'num_simulation_ticks'} = 0;
    $variable_check{'min_pre_execution_sleep_interval'} = 0;
    $variable_check{'max_pre_execution_sleep_interval'} = 0;
    $variable_check{'min_post_execution_sleep_interval'} = 0;
    $variable_check{'max_post_execution_sleep_interval'} = 0;
    $variable_check{'staggered_launch_window_secs'} = 0;
    $variable_check{'min_staggered_launch_window_interval'} = 0;
    $variable_check{'max_staggered_launch_window_interval'} = 0;
    $variable_check{'query_filename_extension'} = 0;
    $variable_check{'max_per_query_executions'} = 0;
    $variable_check{'debug'} = 0;
    $variable_check{'create_required_directories'} = 0;
    $variable_check{'db_name'} = 0;
    $variable_check{'actually_run_query_against_db'} = 0;
    $variable_check{'os_type'} = 0;
    $variable_check{'base_dir'} = 0;
    $variable_check{'qd'} = 0;

    # Process the config file.
    open(CONFIG,$config_file) or die $current_sub . ": Couldn't open config file $config_file $!\n";

    while(<CONFIG>) {
        next if $_ =~ /^#/;

        chomp $_;
        my $data_row = $_;

        my @data = split(/\s+/,$data_row);

        # Ignore any empty rows.
        next if($#data < 0);

        # Switches for the different variables.
        # Query Directory.
        if($data[0] =~ 'qd') {
            # Confirm that the correct number of fields exist per row.
            if($#data != 5) {
                my_print ($current_sub,
                          "Exiting: There is the wrong number of fields in this row:\n\t$data_row");
                exit 0;
            }
            else {
                # Add the row to the data structure.
                $desired_query_mix{$type_counter}{'dir'}        = $data[1];
                $desired_query_mix{$type_counter}{'style'}      = $data[2];
                $desired_query_mix{$type_counter}{'qty'}        = $data[3];
                $desired_query_mix{$type_counter}{'user'}       = $data[4];
                # Below is an addition for Biddle. If there are 10 SQL statements in the 
                # directory, but you only want to use a random 4 of them, set max_sql
                # to 4. Set -1 to use all queries in the directory.
                $desired_query_mix{$type_counter}{'max_sql'}    = $data[5];
            }
            $variable_check{'qd'} = 1;
            $type_counter++;
        }
        else {

            if($debug) { my_print ($current_sub, "Config file parameter $data[0] = $data[1]"); }

            if($data[0] =~ 'debug') { $debug = $data[1]; $variable_check{'debug'} = 1; }

            if($data[0] =~ 'num_simulation_ticks')                 { $num_simulation_ticks = $data[1];
                                                                     $variable_check{'num_simulation_ticks'} = 1; }
            if($data[0] =~ 'min_pre_execution_sleep_interval')     { $min_pre_execution_sleep_interval = $data[1];
                                                                     $variable_check{'min_pre_execution_sleep_interval'} = 1; } 
            if($data[0] =~ 'max_pre_execution_sleep_interval')     { $max_pre_execution_sleep_interval = $data[1];
                                                                     $variable_check{'max_pre_execution_sleep_interval'} = 1; }
            if($data[0] =~ 'min_post_execution_sleep_interval')    { $min_post_execution_sleep_interval = $data[1];
                                                                     $variable_check{'min_post_execution_sleep_interval'} = 1; }
            if($data[0] =~ 'max_post_execution_sleep_interval')    { $max_post_execution_sleep_interval = $data[1];
                                                                     $variable_check{'max_post_execution_sleep_interval'} = 1; }
            if($data[0] =~ 'staggered_launch_window_secs')         { $staggered_launch_window_secs = $data[1];
                                                                     $variable_check{'staggered_launch_window_secs'} = 1; }
            if($data[0] =~ 'min_staggered_launch_window_interval') { $min_staggered_launch_window_interval = $data[1];
                                                                     $variable_check{'min_staggered_launch_window_interval'} = 1; }
            if($data[0] =~ 'max_staggered_launch_window_interval') { $max_staggered_launch_window_interval = $data[1];
                                                                     $variable_check{'max_staggered_launch_window_interval'} = 1; }
            if($data[0] =~ 'query_filename_extension')      { $query_filename_extension = $data[1];
                                                              $variable_check{'query_filename_extension'} = 1; }
            if($data[0] =~ 'max_per_query_executions')      { $max_per_query_executions = $data[1];
                                                              $variable_check{'max_per_query_executions'} = 1; }
            if($data[0] =~ 'create_required_directories')   { $create_required_directories = $data[1];
                                                              $variable_check{'create_required_directories'} = 1; }
            if($data[0] =~ 'db_name')                       { $db_name = $data[1];
                                                              $variable_check{'db_name'} = 1; }
            if($data[0] =~ 'actually_run_query_against_db') { $actually_run_query_against_db = $data[1];
                                                              $variable_check{'actually_run_query_against_db'} = 1; }
            if($data[0] =~ 'os_type')             { $os_type = $data[1];
                                                    $variable_check{'os_type'} = 1; }
            if($data[0] =~ 'base_dir')            { $base_dir = $data[1];
                                                    $variable_check{'base_dir'} = 1; }
            if($data[0] =~ 'sql_run_mode')        { $sql_run_mode = $data[1];
                                                    $variable_check{'sql_run_mode'} = 1; }
            if($data[0] =~ 'kill_full_filename')  { $kill_full_filename = $data[1];
                                                    $variable_check{'kill_full_filename'} = 1; }
            if($data[0] =~ 'output_to_db')        { $output_to_db = $data[1];
                                                    $variable_check{'output_to_db'} = 1; }
            if($data[0] =~ 'output_schema')       { $output_schema = $data[1];
                                                    $variable_check{'output_schema'} = 1; }
            if($data[0] =~ 'output_table')        { $output_table = $data[1];
                                                    $variable_check{'output_table'} = 1; }
            if($data[0] =~ 'output_rg_cfg_table') { $output_rg_cfg_table = $data[1];
                                                    $variable_check{'output_rg_cfg_table'} = 1; }

        }
    } # end while

    close(CONFIG);

    # Confirm that all values have been set.
    foreach my $passed_var (sort keys %variable_check) {
        if($variable_check{$passed_var} == 0) {
            my_print ($current_sub, "Missing configuration variable: $passed_var");
            $retval = 0;
        }           
    }

    if($retval == 0) {
            my_print ($current_sub, "Cannot proceed.");
            exit 1;
    }
    #
    # If running in an 'explain' mode, automatically set the max_per_query_executions to 1.
    if($sql_run_mode > 1) { $max_per_query_executions = 1; }
} # process_config_file


sub initialize_directories {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];

    # Remove any pre-existing runtime SQL files.
    my $delete_runtime_sql_cmd = "rm -f $stored_runtime_sql_directory" . "/*";
    system($delete_runtime_sql_cmd);

    # Remove any pre-existing output filenames.
    my $delete_output_filename_cmd = "rm -f $query_output_directory" . "/*";
    system($delete_output_filename_cmd);
} # initialize_directories


sub load_variable_placement_files {
    
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    opendir(VARS_DIR, $set_variable_replacement_directory)
        or die $current_sub . ": Could not open variable replacement directory $set_variable_replacement_directory $!\n";
    my @var_files = readdir(VARS_DIR);
    closedir(VARS_DIR);
 
    foreach my $x (@var_files) {     
        next unless ($x =~ /[0-9]/ or uc($x) =~ /[A-Z]/);

        my @temp_array;

        # Open the data file and place its contents into an array.
        open(VARS_DATA,$set_variable_replacement_directory . "/" . $x)
            or die $current_sub . ": Could not open the variable replacement file " . $set_variable_replacement_directory . "/" . $x . "$!\n";
        while(<VARS_DATA>) {
            chomp $_;
            push(@temp_array,$_);           
        }
        close(VARS_DATA);

        # Put the array as a value in a hash keyed on the filename.
        $variable_replacement{$x} = \@temp_array;
    }
} # load_variable_placement_files


sub get_query_type {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($query_dir) = @_;
    my $retval; 

    my @query_type_helper = split(/\//, $query_dir);
    my $max_index = $#query_type_helper;
    $retval = $query_type_helper[$max_index];
    if ($debug) { my_print ($current_sub, "----- Query Type: $retval"); }

    return $retval;
} # get_query_type


sub display_query_mix {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($print_bool) = @_;

    my $query_type;

    if($print_bool) { my_print ($current_sub, "About to run the following:"); }
    my $total_query_count = 0;

    foreach my $query_dir (sort(keys %desired_query_mix)) { 

        my $query_type = get_query_type($desired_query_mix{$query_dir}{'dir'});
        
        if($valid_types{$query_dir} == 1) {

            $total_query_count = $total_query_count + $desired_query_mix{$query_dir}{'qty'};

            if($print_bool) { 
                my_print ($current_sub, "\tID: $query_dir "); 
                my_print ($current_sub, "Type: $query_type ");
                my_print ($current_sub, "Concurrency: $desired_query_mix{$query_dir}{'qty'} ");
                if ($desired_query_mix{$query_dir}{'max_sql'} > -1) {
                    my_print ($current_sub, "[Using a randomly selected $desired_query_mix{$query_dir}{'max_sql'} SQL statements from the directory.] ");
                }
                my_print ($current_sub, "Style: $desired_query_mix{$query_dir}{'style'} ");
                my_print ($current_sub, "User: $desired_query_mix{$query_dir}{'user'}\n");
            }
        }
    }
    #print "For a total of $total_query_count number of simultaneous queries.\n";
    #if($print_bool) { my_print ($current_sub, "\n"); }

    return $total_query_count;
} # display_query_mix


sub register_query {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($type, $query, $sql) = @_;

    my_print ($current_sub, "Registering Query => Type: $type, Query_ID: $query");
    if ($debug == 2) { my_print ($current_sub, "\nSQL:\n$sql\n"); }

    $queries{$type}{$query}{'sql'} = $sql;
    $queries{$type}{$query}{'running'} = 0;
} # register_query


sub discover_queries {

    my $current_sub = (split(/::/,(caller(0))[3]))[-1];

    # Go through each query type directory, adding any found queries
    # to the general queries data structure.
    foreach my $type_id (sort(keys %desired_query_mix)) {

        $valid_types{$type_id} = 1;

        my $query_type_dir = $desired_query_mix{$type_id}{'dir'};
        opendir(TYPE_DIR, $query_type_dir)
            or die $current_sub . ": FAIL -> Could not open query directory $query_type_dir $!\n";
        my @temp_queries = readdir(TYPE_DIR);
        closedir(TYPE_DIR);
    
        if ($debug) { my_print ($current_sub, "Query Directory: $query_type_dir"); }

        # Use a subset of all the queries in a directory are to be used,
        # randomly select the right number.
        if ($desired_query_mix{$type_id}{'max_sql'} > -1) {

            my $num_needed_queries = $desired_query_mix{$type_id}{'max_sql'};
            my $total_queries = 0;

            my @counted_queries;

            foreach my $x (@temp_queries) {            
                next unless ($x =~ /[0-9]/ or uc($x) =~ /[A-Z]/);
                if($query_filename_extension =~ 'NONE') {
                    next unless $x =~ /$query_filename_extension$/;
                }
                push(@counted_queries,$x);
                $total_queries++;
            }

            my @random_queries;
            my %already_chosen;

            while ($num_needed_queries > 0) {
                my $random_pos = -1;    
                my $new_random_number = 0;

                while($new_random_number == 0) {            
                
                    $random_pos = int rand($total_queries);
                    if(exists($already_chosen{$random_pos})) { 
                        $new_random_number = 0; 
                    }
                    else {
                        $already_chosen{$random_pos} = 1;
                        $new_random_number = 1;
                    }
                }

                push(@random_queries, $counted_queries[$random_pos]);
                $num_needed_queries--;
            }

            @temp_queries = @random_queries;
        }
    
        # Register each query in the general queries data structure.
        foreach my $query (@temp_queries) {

            next unless ($query =~ /[0-9]/ or uc($query) =~ /[A-Z]/);
            if($query_filename_extension =~ 'NONE') {
                next unless $query =~ /$query_filename_extension$/;
            }
    
            if($debug) { my_print ($current_sub, "Adding query from file: $query"); } 
            my $full_path = $query_type_dir . $query;
            my $sql = "";
            open(SQL,$full_path) or die $current_sub . ": Could not open query: $full_path $!\n";
            while(<SQL>) {
                $sql = $sql . $_;
            }
            close(SQL);

            register_query($type_id, $query, $sql);
        }
    }
} # discover_queries


sub display_running_status {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($total_queries) = @_;

    my @running_queries = sort (keys(%pids));
    my $num_queries = 0;
    my_print ($current_sub, "\tStatus[" . ($#running_queries + 1) . " of max $total_queries]: "); 

    my $running_query_list = "";
    foreach my $running_query (@running_queries) { 
        $running_query_list =  "$running_query, $running_query_list";
        $num_queries++; 
    }
    my_print ($current_sub, "Num of queries: $num_queries ; Running queries: $running_query_list");
} # display_running_status


sub check_available_slots_for_type {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($type) = @_;
    my $retval = 0;

    # Check the number of queries currently executing for the type.
    my $num_executing_queries = 0;
    my @queries = keys(%{$queries{$type}});
    foreach my $query (@queries) {
        my @pids = keys(%{$queries{$type}{$query}{'pid'}});
        foreach my $pid (@pids) {
            if($queries{$type}{$query}{'pid'}{$pid} > 0) {
                $num_executing_queries = $num_executing_queries + $queries{$type}{$query}{'pid'}{$pid};
            }
        }
    }

    # Check the maximum number of concurrent queries for the type.
    my $total_queries_for_type = 0;
    $total_queries_for_type = $total_queries_for_type + $desired_query_mix{$type}{'qty'};

    if ($debug) (my_print ($current_sub, ": Type = $type: there are $numb_executing_queries out of $total_queries_for_type executing"); }

    # Decide if there are available slots for the type.
    if($num_executing_queries < $total_queries_for_type) {
        $retval = 1;
    }

    return $retval;
} # check_available_slots_for_type 


sub check_query_counts {

    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my @types = keys(%queries);
    if($debug) {
        foreach my $type (@types) {
            my_print ($current_sub, "**********************************************\nType: $type\t");
            my @queries = sort keys(%{$queries{$type}});
            foreach my $query (@queries) {
                my @pids = keys(%{$queries{$type}{$query}{'pid'}});
                my_print ($current_sub, "Query: $query\t" . ($#pids + 1));
            }
            my_print ($current_sub, "*********************************************");
        }
    }

} # check_query_counts


sub get_valid_types {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my @retval;

    my $type_list = "";
    foreach my $type (keys(%valid_types)) {
        if($valid_types{$type} == 1) {
            push(@retval, $type);
            $type_list = "$type\t$type_list";
        }
    }

    if($debug) { my_print ($current_sub, "-------------- Valid types: $type_list") ; }

    return @retval;
} # get_valid_types


sub select_next_query {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($pid) = @_;

    my $need_next_query = 1;    
    my $next_query_type;
    my $next_query_id = 0;
    my $next_query_sql;
    my %num_types_checked;

    # Until the next to run has been selected, or there are no more slots currently available.
    my $header="--------------"; my $footer="";
    while($need_next_query) {
        # Randomly pick a query type, from remaining valid types.
        my @types = get_valid_types();

        if($debug) { my_print ($current_sub, "$header Number of elements in types array: $#types $footer"); }

        my %type_map;
        for(my $y = 0; $y <= $#types; $y++) {
            $type_map{$y} = $types[$y];
            if($debug) {  my_print ($current_sub, "$header Map($y): $types[$y] $footer"); }
        }

        my $num_types = ($#types + 1);
        if($debug) { my_print ($current_sub, "$header Number of types: $num_types $footer"); }

        my $rand_type = int(rand($num_types)) +0;
        if($debug) { my_print ($current_sub, "$header Rand Type: $rand_type $footer"); }

        $next_query_type = $type_map{$rand_type};
        if($debug) { my_print ($current_sub, "$header Type chosen: $next_query_type $footer"); }

        # Hack!
        $rand_type = $next_query_type;

        # See if that type has available slots.
        if(check_available_slots_for_type($next_query_type) == 0) {
            $num_types_checked{$next_query_type} = 1;
            if(keys(%num_types_checked) == $num_types) { return 0; }
            next;
        }

        # Within a type, if the Style is 'random', then randomly pick one of the queries of that type.
        if($desired_query_mix{$rand_type}{'style'} =~ /random/) {
            # Randomly pick a query of that type
            my @queries = keys(%{$queries{$rand_type}});
            my $num_queries = $#queries;
            $num_queries = ($num_queries + 1);
            my $rand_query = int(rand($num_queries));
            $next_query_id = $queries[$rand_query];
            $next_query_sql = $queries{$next_query_type}{$next_query_id}{'sql'};
            $need_next_query = 0;
        }

        # Within a type, if the Style is 'ordered', then iterate through the queries and find one 
        #   with a least number of runs.
        if($desired_query_mix{$rand_type}{'style'} =~ /ordered/) {
            my @queries = sort keys(%{$queries{$rand_type}});
            my $min_query_run_count = 999999999;
            my $min_query_name = "foo";
            my $on_last_query = 0;
            #foreach my $query (@queries) 
            for (my $x = 0; $x <= $#queries; $x++) {
                my $query = $queries[$x];
                my @num_pids = keys(%{$queries{$rand_type}{$query}{'pid'}});

                if($#num_pids < $min_query_run_count) {
                    $min_query_run_count = $#num_pids;
                    $min_query_name = $query;

                    if($x == $#queries) { $on_last_query = 1; }
                }
            }

            # If the maximum number of query executions has been reached, end the simulation early.
            if($max_per_query_executions > 0) {
                if($on_last_query) {

                    # There is a bit of math here, because we are counting the highest index
                    # in the hash containing the pids.
                    if($debug) { 
                        my_print ($current_sub,
                            "For $min_query_name, the (corrected) min_query_run_count is " . ($min_query_run_count + 2) . 
                        " and the max_per_query_executions is $max_per_query_executions");
                    }
                    if(($min_query_run_count + 2) >= $max_per_query_executions) {
                        if($debug) { my_print ($current_sub, "Invalidating query type $next_query_type \n"); }
                        $valid_types{$next_query_type} = 0;

                        # Force the reload of the tot_queries value from the main loop.
                        $reload_tot_queries = 1;

                        if($#types < 1) { 
                            if($debug) { my_print ($current_sub, "Setting the end simulation early flag."); }
                            $end_simulation_early = 1; 
                            #return 0;
                        }  

                        if($debug) { 
                            my_print ($current_sub, "Max executions per query reached, sending early-end-simulation flag."); 
                        }  
                    }
                }
            }

            $next_query_id = $min_query_name;
            $next_query_sql = $queries{$next_query_type}{$next_query_id}{'sql'};
            $need_next_query = 0;
        }

    }
    # Once the next one to run has been chosen, add its pid to the query.
    $queries{$next_query_type}{$next_query_id}{'pid'}{$pid} = -999; 
    if($debug) { 
        my_print ($current_sub,
                  "\tSELECT:\t\t(Runnable) Setting $next_query_type $next_query_id $pid to " . 
                  $queries{$next_query_type}{$next_query_id}{'pid'}{$pid});
    }
    return 1;
} # select_next_query


sub start_next_query {

    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    if ($debug) { my_print ($current_sub, "***** ENTERING start_next_query() *****"); }
    my $next_query_type;
    my $next_query_id = 0;
    my $next_sql;
    my $next_pid;
    my $found_next_query = 0;

    my $counter = 0;

    # Traverse the Queries hash for a pid with a value of -999.
    my @types = keys(%queries);
    foreach my $type (@types) {
        my @queries = keys(%{$queries{$type}});
        foreach my $query (@queries) {
            my @pids = keys(%{$queries{$type}{$query}{'pid'}});
            foreach my $pid (@pids) {
                if($queries{$type}{$query}{'pid'}{$pid} == -999) {
                    if($debug) {
                        my_print ($current_sub,
                            "\tSTART:\t\t(Executing) Found pid $pid with current status of " .
                            $queries{$type}{$query}{'pid'}{$pid}) ;
                    }
                    $counter++;
                    $next_query_type = $type;
                    $next_query_id = $query;
                    $next_sql = $queries{$type}{$query}{'sql'};
                    $next_pid = $pid;

                    # Set the pid's status to 1.
                    $queries{$type}{$query}{'pid'}{$pid} = 1;

                    if($debug) {
                        my_print ($current_sub,
                            "Setting status to $queries{$type}{$query}{'pid'}{$pid}.\n" .
                            "Extra TYPE $next_query_type ID $next_query_id SQL $next_sql") ;
                    }

                    $found_next_query = 1;
                }
            }
        }
    }

    return ($next_query_type, $next_query_id, $next_sql, $next_pid, $found_next_query);
} # start_next_query


sub complete_query {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($pid,$query_type) = @_;
    my $found_pid = 0;

    # Traverse the Queries hash in search of the pid of interest.
    my @queries = keys(%{$queries{$query_type}});
    foreach my $query (@queries) {
        if(exists($queries{$query_type}{$query}{'pid'}{$pid})) {
            $queries{$query_type}{$query}{'pid'}{$pid} = 0;
            $found_pid = 1;
        }
    }

    check_query_counts();

    return $found_pid;
} # complete_query


sub handle_sql_set_variable_replacement {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($original_sql) = @_;
    #my $original_sql = @_;
    my $retval = $original_sql;
    my $found_set = 0; 

    # Detect the presence of the variable file or SET variables.
    if($original_sql =~ /\/\*\+VARIABLE_REPLACEMENT_FILENAME=.+\*\//i) {
        if($debug) { my_print ($current_sub, "********************** SET MATCH !!!!!"); }    
        $found_set = 1; 
    }            

    if($found_set) {

        # Open the variable file.
        my ($garbage1, $good1) = split(/\*\+VARIABLE_REPLACEMENT_FILENAME=/,$original_sql);
        my ($filename, $garbage2) = split(/\*\//,$good1);
                     
        if($debug) { my_print ($current_sub, "*** Here's the replacement filename:$filename"); }

        # Get a random row from the variable file.
        my $max_index = ( @{$variable_replacement{$filename}} -1);
    
        my $random_row_pos = rand($max_index);
        my @random_values = split(/\t/,$variable_replacement{$filename}[$random_row_pos]);

        # Replace the SET variables with the values from the row in the variable file.
        for(my $x = 0; $x <= $#random_values; $x++) {

            my $search_string = "\\" . "\$" . ($x + 1);
            if($debug) {
                my_print ($current_sub, "About to replace $search_string with " . $random_values[$x]);
            }    

            $retval =~ s/$search_string/$random_values[$x]/g;
        }    
    }    

    # Return the SQL, either with the replacement done (if needed) or simply the original SQL. 
    if($debug) { my_print ($current_sub, "ABOUT TO RETURN $retval ***"); }

    return $retval;
} # handle_sql_set_variable_replacement 


sub handle_sql_run_mode {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($original_sql) = @_;
    my $retval = $original_sql;

    # 1 = Run the SQL as-is.
    # No need for sql_run_mode = 1, we just return the original_sql.

    # 2 = Uncomment an EXPLAIN or EXPLAIN ANALYZE stored in a comment of the form:
    #     /*+EXPLAIN_STATEMENT_STRING=EXPLAIN ANALYZE*/
    if($sql_run_mode =~ /2/) {
        $retval =~ s/\/\*\+EXPLAIN_STATEMENT_STRING=EXPLAIN ANALYZE\*\// EXPLAIN ANALYZE /i;
        $retval =~ s/\/\*\+EXPLAIN_STATEMENT_STRING=EXPLAIN\*\// EXPLAIN /i;
    }

    # 3 = Have the program place a ' EXPLAIN ' string before the first occurrance of 'SELECT'.
    if($sql_run_mode =~ /3/) {
        $retval =~ s/SELECT/ EXPLAIN SELECT/i;
    }

    # 4 = Have the program place a ' EXPLAIN ANALYZE ' string before the first occurrance of 'SELECT'.
    if($sql_run_mode =~ /4/) {
        $retval =~ s/SELECT/ EXPLAIN ANALYZE SELECT/i;
    }

    return $retval;
} # handle_sql_run_mode

sub run_query {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($query_type, $query_id, $sql, $pid) = @_;
    my $elapsed_secs;
    my $run_status;

    if($debug) {
        my_print ($current_sub, "\tRUN:\t\t(Running) job_run $pid and TYPE $query_type ID $query_id");
    }

    # Label the query with a flag for when it launched:
    # (S)taggered launch window, or (N)ormal execution phase. 
    # (A)fter normal isn't possible for when the query was launched,
    # only for when it completed.
    my $launch_flag = "N"; 

    # Similarly, create a flag for when the query completes.
    my $complete_flag = "N";

    # If requested, sleep for an interval during the beginning phase of the test.
    if($staggered_launch_window_secs > 0) {

        $elapsed_secs = ($num_simulation_ticks - ($end_simulation_ticks - $current_simulation_ticks));

        if($elapsed_secs < $staggered_launch_window_secs) {
            my $staggered_launch_sleep_interval;

            if($debug) {
                my_print ($current_sub, "In launch window with $elapsed_secs elapsed seconds of a " .
                          "$staggered_launch_window_secs second launch window!") ;
            }

            if($min_staggered_launch_window_interval    == $max_staggered_launch_window_interval) {
                $staggered_launch_sleep_interval = $max_staggered_launch_window_interval;
            }
            else {
                my $random_adjustment = int(rand($max_staggered_launch_window_interval
                                                 - $min_staggered_launch_window_interval));
                $staggered_launch_sleep_interval  = ($min_staggered_launch_window_interval
                                                     + $random_adjustment);
            }


            if($debug) {
                my_print ($current_sub,
                    "LAUNCH WINDOW SLEEP (" . $staggered_launch_sleep_interval . " secs)");
            }

            system("sleep " . $staggered_launch_sleep_interval);
        }
    }



    # If requested, sleep for an interval before executing the query. 
    if($max_pre_execution_sleep_interval > 0) {

        my $pre_exec_sleep_interval;

        # If the min and max values are the same, sleep for a static (not random amount) of time.
        if($min_pre_execution_sleep_interval    == $max_pre_execution_sleep_interval) {
            $pre_exec_sleep_interval   = $max_pre_execution_sleep_interval;
        }
        else {  
            my $random_adjustment = int(rand($max_pre_execution_sleep_interval - $min_pre_execution_sleep_interval));
            $pre_exec_sleep_interval = ($min_pre_execution_sleep_interval + $random_adjustment);
        }

        if($debug) {
            my_print ($current_sub, "PRE-EXECUTE SLEEP (" . $pre_exec_sleep_interval . " secs).\n");
        }

        system("sleep " . $pre_exec_sleep_interval);
    }

    if ($debug) { print "Type: $query_type\nID: $query_id\nSQL: $sql\n"; }

    # Copy the SQL to the stored_sql dir (handle wildcards eventually here too).
    my $runtime_sql_filename = $stored_runtime_sql_directory . "runtime_sql_" .
        $query_type . "_" . $query_id . "_" . $pid;

    # Check for SET variable replacement and perform the replacement if needed.
    $sql = handle_sql_set_variable_replacement($sql);

    # If requested, use the EXPLAIN version of the SQL.
    $sql = handle_sql_run_mode($sql);

    # Write the runtime SQL file.
    open(SQL,">$runtime_sql_filename")
        or die "Could not open $runtime_sql_filename for writing $!\n";
    print SQL $sql;
    close(SQL);

    # Build the SQL command.
    my $output_filename = $query_output_directory . "output_" . $query_type . "_" .
                          $query_id . "_" . $pid;

    # Build the log filename.
    my $log_filename = $query_log_directory . "log_" . $query_type . "_" . $query_id . "_" . $pid;

    # Build the actual SQL command.
    my $cmd = "psql -q -A -t -v ON_ERROR_STOP\=1 -d $db_name -f $runtime_sql_filename " .
                   "-o $output_filename -h localhost -U " . $desired_query_mix{$query_type}{'user'};

    # Capture the run ID and start_time.
    my $before_timestamp = get_current_datetime();
    $before_timestamp =~ s/_/\t/;
    my $output =  $query_type . "\t" . $query_id . "\t" . $query_type . "_" .
                  $query_id . "_" . $pid . "\t" . $before_timestamp; 

    # Open the log file for writing.
    open(LOG,">$log_filename") or die "Can't open log file $log_filename for writing $!\n";

    print LOG $output;

    close(LOG);

    if($staggered_launch_window_secs > 0) {
        # Recompute the elapsed seconds.
        $elapsed_secs = ($num_simulation_ticks - ($end_simulation_ticks - get_epoch_seconds()));

        if($debug) {
            my_print ($current_sub, "Elapsed secs (Launch): $elapsed_secs and staggered_launch_window_secs: $staggered_launch_window_secs");
        }

        # Change the launch flag to (S)taggered launch window if the query is
        # still in that window.
        if($elapsed_secs < $staggered_launch_window_secs) {
            $launch_flag = "S";

            if($debug) { my_print ($current_sub, "Setting launch flag to $launch_flag"); }
        }
    }

    # Run the query.
    if($actually_run_query_against_db) {
        system($cmd);
        sleep 2;

        $run_status = ($? >> 8);
        if($run_status == 0) { $run_status = 1; }
        if($run_status == 3) { $run_status = 0; }
    }

    # If selected, just sleep a little after running the bogus query.
    if(!$actually_run_query_against_db) {
        #my $rand = (int(rand(4)) +1);
        my $rand = 4;
        sleep($rand);
        $run_status = 1;
    }

    # Take a timestamp for end_time.
    my $end_timestamp = get_current_datetime();
    $end_timestamp =~ s/_/\t/;
    $output = "\t" . $end_timestamp;

    # Capture the number of rows in the output file.
    my $num_rows = 0;
    if($actually_run_query_against_db) {
        my $num_rows_cmd = "wc -l $output_filename";
        my $num_rows_string = `$num_rows_cmd`;
        my @num_rows_array = split(/\s+/,$num_rows_string);
        $num_rows = $num_rows_array[0];
    }

    my ($garbage1,$before_secs) = split(/\t/,$before_timestamp);
    my ($garbage2,$end_secs)    = split(/\t/,$end_timestamp);
    my $query_runtime           = $end_secs - $before_secs;

    # Recompute the elapsed seconds.
    $elapsed_secs = ($num_simulation_ticks - ($end_simulation_ticks - get_epoch_seconds()));

    if($debug) { my_print ($current_sub, "Elapsed secs (Complete): $elapsed_secs" .
                       " and staggered_launch_window_secs: $staggered_launch_window_secs"); }
       
    # Label the query with a flag for when it completed:
    #   (S)taggered launch window, (N)ormal execution phase,
    #   or (A)fter normal execution phase.
    # Change the complete flag to (S)taggered launch window if the query completes
    # in that window.
    if($elapsed_secs < $staggered_launch_window_secs) { $complete_flag = "S"; }

    # Change the complete flag to (A)fter normal execution phase if the query
    # completes in that window.
    if(get_epoch_seconds() > $end_simulation_ticks) { $complete_flag = "A"; }

    if($debug) { my_print ($current_sub, "Setting complete flag to $complete_flag"); }

    $output = $output . "\t" . $num_rows . "\t" . "1" . "\t" . $query_runtime . "\t" .
              $launch_flag . "\t" . $complete_flag . "\t"  . $run_status . "\t" .
              $right_now . "\t" . "$nickname\n";

    # Open the log file for appending.
    open(LOG,">>$log_filename") or die $current_sub . ": Can't open log file $log_filename for writing $!\n";

    print LOG $output;

    # Close the log file.
    close(LOG);

    # If requested, sleep for an interval after executing the query.       
    if($max_post_execution_sleep_interval > 0) {

        my $post_exec_sleep_interval;

        # If the min and max values are the same, sleep for a static (not random amount) of time.
        if($min_post_execution_sleep_interval == $max_post_execution_sleep_interval) {
            $post_exec_sleep_interval = $max_post_execution_sleep_interval;
        }
        else {
            $random_adjustment = int(rand($max_post_execution_sleep_interval
                                          - $min_post_execution_sleep_interval));
            $post_exec_sleep_interval = ($min_post_execution_sleep_interval + $random_adjustment);
        }

        if($debug) {
            my_print ($current_sub, "POST-EXECUTE SLEEP (" . $post_exec_sleep_interval . " secs)");
        }

        system("sleep " . $post_exec_sleep_interval);
    }

} # run_query


sub get_current_datetime {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my $retval;

    # Perhaps no need for nanoseconds on the human-readable date, but add it anyway.
    $retval = `date "+%Y/%m/%d %H:%M:%S_%s.%N"`;
    chomp $retval;

    # Mac OS doesn't have the nanoseconds 'N', so trim it. So, running on Mac you only get seconds resolution, but full nanosec resolution on Linux.
    $retval =~ s/\.N//g;
    
    return $retval;
} # get_current_datetime


sub get_epoch_seconds {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my $retval;
    my $cmd;


    if($os_type =~ /mac/) {
        # This version works on Mac OS.
        $cmd = "date -j -f \"%a %b %d %T %Z %Y\" \"`date`\" \"+%s\"";
    }
    else {

        # Other platform version.
        $cmd = "date +\"%s.%N\"";
    }

    $retval = `$cmd`;

    chomp $retval;  
    
    return $retval;
} # get_epoch_seconds


sub get_seconds {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my $retval;
    my $cmd;


    if($os_type =~ /mac/) {
        # This version works on Mac OS. 
        $cmd = "date +\"%s\"";
    }   
    else {
        # Other platform version.
        $cmd = "date +\"%s\"";
    }   

    $retval = `$cmd`;

    chomp $retval;  
    
    return $retval;
} # get_seconds


sub create_pivoted_summary_report {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($summary_filename) = @_;

    my %answer;
    my %retval;
    my $query_key;

    # 0 QueryType
    # 1 QueryID
    # 2 QueryRunID
    # 3 StartTimeHuman
    # 4 StartTimeEpoch
    # 5 EndTimeHuman
    # 6 EndTimeEpoch
    # 7 NumRows
    # 8 Helper
    # 9 Runtime
    # 10 Launch Flag
    # 11 Complete Flag
    # 12 Success Flag
    # 13 RunName
    # 14 RunNickname

    # Open the file twice to avoid having to store the numbers in an array in the hash... a little lazy.
    open(SUMM_DATA_IN, $summary_filename) or die $current_sub . ": Can't open the summary data (IN) $summary_filename $!\n";
    while(<SUMM_DATA_IN>) {
        next if $_ =~ /Q/;
    
        chomp $_;
        my @row = split(/\t/,$_);

        $query_key = $row[1] . "\t" . $row[12];

        # Ignore data rows that don't have all of the data. This could be improved when failed
        #    queries are better detected.
        if($#row == 14) {
            # Compute the number of executions per query.
            $answer{$query_key}{'counter'}++;

            # Compute the total runtime per query.
            if(exists($answer{$query_key}{'sum'})) {
                $answer{$query_key}{'sum'} = $answer{$query_key}{'sum'} + $row[9];
            }
            else {
                $answer{$query_key}{'sum'} = $row[9];
            }
        }
    } 
    close(SUMM_DATA_IN);


    open(SUMM_DATA_IN, $summary_filename)
                or die "Can't open the summary data (IN) $summary_filename $!\n";
    while(<SUMM_DATA_IN>) { #if($debug) { print "SUMDATA:" . $_; }
       next if $_ =~ /Q/;

       chomp $_;
       my @row = split(/\t/,$_);

       $query_key = $row[1] . "\t" . $row[12];

       # Ignore data rows that don't have all of the data. This could be improved
       # when failed queries are better detected.
       if($#row == 14) {

            $retval{$query_key}{'num_runs'} = $answer{$query_key}{'counter'};
            $retval{$query_key}{'average'} = ($answer{$query_key}{'sum'} / $answer{$query_key}{'counter'});

            #if($debug) { print "AVG $row[0] :" . $retval{$query_key}{'average'} . "\n"; }
    
            if(exists($retval{$query_key}{'square_difference'})) {
                $retval{$query_key}{'square_difference'} += ($retval{$query_key}{'average'} - $row[9])**2;
            }
            else {
                $retval{$query_key}{'square_difference'} = ($retval{$query_key}{'average'} - $row[9])**2;
            }
    
            # Compute the min() runtime per query.
            if(exists($retval{$query_key}{'min'})) {
                if($row[9] < $retval{$query_key}{'min'}) {
                     $retval{$query_key}{'min'} = $row[9];
                }
            }
            else {
                $retval{$query_key}{'min'} = $row[9];
            }

            # Compute the max() runtime per query.
            if(exists($retval{$query_key}{'max'})) {
                if($row[9] > $retval{$query_key}{'max'}) {
                     $retval{$query_key}{'max'} = $row[9];
                }
            }
            else {
                $retval{$query_key}{'max'} = $row[9];
            }

        }
    }
    close(SUMM_DATA_IN);

    my $pivot_filename = $summary_filename;
    $pivot_filename =~ s/\.dat/_pivot.dat/g;

    open(OUTPUT, ">$pivot_filename") or die $current_sub . ": Can't open the pivot output filename $pivot_filename $!\n";

    print OUTPUT "Query\tSuccess\tNumRuns\tMeanRuntime\tMinRuntime\tMaxRuntime\tStdDev\tRelativeDev\tRunName\tRunNickname\n";

    foreach my $query (sort(keys(%retval))) {

        print OUTPUT    
            "$query\t" .
            "$retval{$query}{'num_runs'}\t" .
            "$retval{$query}{'average'}\t" .
            "$retval{$query}{'min'}\t" .
            "$retval{$query}{'max'}\t";
        print OUTPUT sqrt($retval{$query}{'square_difference'} / $retval{$query}{'num_runs'}) . "\t";
        print OUTPUT ((sqrt($retval{$query}{'square_difference'} / $retval{$query}{'num_runs'})) / $retval{$query}{'average'}) . "\t";
        print OUTPUT $right_now . "\t";
        print OUTPUT $nickname . "\n";
    }

    close(OUTPUT);

    my_print ($current_sub, "A pivoted version of the summary file is located here: " . $pivot_filename);

} # create_pivoted_summary_report 


sub clean_up {

    my $current_sub = (split(/::/,(caller(0))[3]))[-1];

    # Remove any runs that started too late, and should not be counted.
    my $delete_late_runs_cmd = "rm -f $query_log_directory" . "log__*";
    system($delete_late_runs_cmd);
    
    # Consolidate all of the log files for this run into a single file.
    my $consolidate_cmd = "cat $query_log_directory" . "/* > " . $exec_summary_directory .
            "execution_summary_" . $right_now . ".dat";
    system($consolidate_cmd);
      
    # Load the output data into a database, if needed.
    if($output_to_db) {
        my $source_filename = $exec_summary_directory . "execution_summary_" . $right_now . ".dat";
        my $sql_cmd = "\\COPY $output_schema.$output_table FROM \'$source_filename\' HEADER NULL AS \'NULL\'";
        my $sql_cmd_status = run_sql($sql_cmd);
        my_print ($current_sub, "COPY: $sql_cmd");

# If resource groups are being used, save that info also
        if (length($output_rg_cfg_table) > 0) {
            $sql_cmd = "insert into $output_schema.$output_rg_cfg_table
                             select '$nickname'
                                  , array(select rolname || ':' || rsgname
                                          from pg_roles, pg_resgroup
                                          where pg_roles.rolresgroup=pg_resgroup.oid
                                            and rolname like '%user%')
                                  , g.*
                             from gp_toolkit.gp_resgroup_config g";
            $sql_cmd_status = run_sql($sql_cmd);
            my_print ($current_sub, "INSERT: $sql_cmd");
        }
    }

    # Put a header row into the file.
    my $header_filename = $exec_summary_directory . "execution_summary_" . $right_now . ".dat";
    my $header_filename_temp = $exec_summary_directory . "execution_summary_" . $right_now . ".datTEMP";
    open(SUMMARY_NEW,">$header_filename_temp")
        or die $current_sub . ": Can't open log file $header_filename_temp for writing $!\n";
    open(SUMMARY_OLD, $header_filename)
        or die $current_sub . ": Can't open the summary file for reading $header_filename $!\n";
    
    print SUMMARY_NEW   "QueryType\t".
                "QueryID\t" .
                "QueryRunID\t" .
                "StartTimeHuman\t" .
                "StartTimeEpoch\t" . 
                "EndTimeHuman\t" .
                "EndTimeEpoch\t" .
                "NumRows\t" .
                "Helper\t" .
                "Runtime\t" .
                "LaunchFlag\t" .
                "CompleteFlag\t" .
                "SuccessFlag\t" .
                "RunName\t" .
                "RunNickname\n";
    while(<SUMMARY_OLD>) {
        print SUMMARY_NEW $_;
    } 

    close(SUMMARY_OLD);
    close(SUMMARY_NEW); 

    my $rename_cmd = "mv $header_filename_temp $header_filename";
    system($rename_cmd);

    my_print ($current_sub, "All log files have been consolidated into summary file here: " . 
        $exec_summary_directory .  "execution_summary_" . $right_now . ".dat");

    create_pivoted_summary_report($header_filename);

    exit 1;
} # clean_up

sub run_sql {
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my ($sql_string) = @_;
    my $retval;

    if($debug) { my_print ($current_sub, "Entering run_sql: SQL command:\n$sql_string"); }

    my $cmd = "psql -v ON_ERROR_STOP\=1 -d $db_name -c \"$sql_string;\"";

    system($cmd);
                
    $retval = ($? >> 8);
        
    if($retval == 0) { $retval = 1; }
    if($retval == 3) { $retval = 0; }

    if($debug) { my_print ($current_sub, "Leaving run_sql: retval = $retval"); }
    
    return $retval;
} # run_sql


sub prepare_output_schema_and_tables {
    
    my $current_sub = (split(/::/,(caller(0))[3]))[-1];
    my $status = 1;

    if($debug) { my_print ($current_sub, "output_to_db = $output_to_db"); }

    if($output_to_db) {
        if($debug) {
            my_print ($current_sub, "Configuring table '$output_schema.$output_table' for test results loading");
        }

        # If the schema doesn't exist, create it.
        my $sql_cmd = "CREATE SCHEMA IF NOT EXISTS $output_schema";
        $status = run_sql($sql_cmd);

        # If the table doesn't exist, create it.
        if ($status == 1) {
            $sql_cmd = "CREATE TABLE IF NOT EXISTS $output_schema.$output_table (
                           queryType        integer,
                           queryId          varchar,
                           queryRunId       varchar,
                           startTimestamp   timestamp,
                           startTimeEpoch   numeric(19,9),
                           endTimestamp     timestamp,
                           endTimeEpoch     numeric(19,9),
                           numRowsReturned  integer,
                           excelPivotHelper integer,
                           Runtime          numeric(8,4),
                           launchFlag       varchar(1),
                           completeFlag     varchar(1),
                           successFlag      integer,
                           runName          varchar,
                           runNickname      varchar
                    )
                    DISTRIBUTED RANDOMLY";
            $status = run_sql($sql_cmd);
        }

        # Record the Resource Groups settings and which resource group a user belongs to
        if ($status == 1 and length($output_rg_cfg_table) > 0) {
            $sql_cmd = "CREATE TABLE IF NOT EXISTS $output_schema.$output_rg_cfg_table (
                               runNickname varchar,
                               users_rg_assignment varchar[],
                               like gp_toolkit.gp_resgroup_config
                        )
                        DISTRIBUTED RANDOMLY";
            $status = run_sql($sql_cmd);
        }
    }
    else {
        if($debug) { my_print ($current_sub, "We don't need to config the schema and table."); }
    }

    if($debug) { my_print ($current_sub, "status = $status"); }
    return $status;

} # prepare_output_schema_and_tables 

my $current_sub = "main";

################################################################################
# Main loop ####################################################################
################################################################################

# Check for the existance of the config file on the command line.
check_config_file();

# Process the configuration file.
process_config_file();

# Check to make sure that all the required directories are in place.
verify_setup();

# Clean the directories about to receive output, logs and runtime_sql.
initialize_directories();

# Load the variable replacement files into memory.
load_variable_placement_files();

# Discover what queries are available.
discover_queries();

# Set up schema and table to record output, as needed.
# Also sets up a table to save the resource group settings and user -> RG assignments.
prepare_output_schema_and_tables();

# Display the intended mix of queries, by type. Also return the total number
# of queries to run at one time (since we compute it for the function already).
my $tot_queries = display_query_mix(1);

sleep 5;

# Start the clock.
$current_simulation_ticks = get_epoch_seconds();
$end_simulation_ticks = ($current_simulation_ticks + $num_simulation_ticks);
my $print_friendly_end_secs = (get_seconds() + $num_simulation_ticks);

my $counter = 0;
my $job_run_counter = 0;
my %has_printed_status;
my $received_ctrl_c = 0;

# Only run the simulation for a certain number of moves or ticks.
while(($current_simulation_ticks < $end_simulation_ticks) && ($end_simulation_early < 1)) {

    $current_simulation_ticks = get_epoch_seconds();

    my $print_friendly_secs = get_seconds();

    if(!$has_printed_status{$print_friendly_secs}) { 
        my_print ($current_sub, "Beginning simulation quanta " . 
            ($num_simulation_ticks + ($print_friendly_secs - $print_friendly_end_secs)). 
            " of $num_simulation_ticks ticks.");
        $has_printed_status{$print_friendly_secs} = 1;

        # Wrap this in a mod() call to output the incremental status.
        display_running_status($tot_queries);
    }

    # Check for the existence of the kill file.
    if(-e $kill_full_filename) { $end_simulation_early = 1; }

    # Maintain a certain number of running jobs.
    while (keys %pids < $tot_queries) {

        if($debug) { my_print ($current_sub, "There are " . keys(%pids) . " pids of $tot_queries"); }

        $counter++;
        $job_run_counter++;

        # Since the return value is constrained to 8-bits; reset the counter.
        if($counter > 254) { $counter = 1 }

        # Find the next query to run.
        my $select_status =  select_next_query($counter);

        # Conditionally reload the tot_queries value.
        if($reload_tot_queries) { 
            my $pre_tot_queries = $tot_queries;
            $tot_queries = display_query_mix(0); 
            if($debug) { my_print ($current_sub, "Reloading tot_queries. OLD: $pre_tot_queries NEW: $tot_queries"); }
        }

        # The resource allocation queue is full, sleep then try again.
        if(!$select_status) { 
            my_print ($current_sub, "The queue is full: No next query chosen:  COUNTER $counter"); 
            sleep 5;
        }

        # For the purposes of the number of running jobs, start the query.
        my ($query_type, $query_id, $sql, $next_pid, $found_next_query) = start_next_query();

        # Fork call.
        die $current_sub . ": could not fork" unless defined(my $pid = fork);

        # The PARENT registers the process on the PIDS hash for maintaining the correct number of jobs.
        if ($pid) {
            $pids{$pid} = $query_type;
            next;
        }

        # The CHILD  executes the SQL returned by the start_next_query() call.
        if($select_status) {
            run_query($query_type, $query_id, $sql, $job_run_counter);
        }
        exit $counter;
    }

    my $pid = waitpid -1, WNOHANG;
    
    # As processes complete, they pass their ID as the return code. This value is used to update the 
    #   PIDS hash in terms of the number of concurrent queries and QUERIES hash capturing which queries 
    #   are running.
    if ($pid > 0) {
        my $query_type = $pids{$pid};

        delete $pids{$pid};

        my $rc = $? >> 8;
        my $found_pid = complete_query($rc,$query_type);
        if($found_pid) {
            # pid successfully removed.
            if($debug) { my_print ($current_sub, "COMPLETE:\t(Complete) pid $rc"); }
        }
        else {
            my_print ($current_sub, "***** Warning: Could not find Running pid $rc to complete.");
        }
        if($debug) { my_print ($current_sub, "END:\t(Finalize) Removing " . $rc . "(" . $pid . ")"); }
    }

} # end while(run simulation)


if($end_simulation_early) {
    if(-e $kill_full_filename) {
        my_print ($current_sub, "*** Kill file detected ($kill_full_filename), exiting early.");
    }
    else {
        my_print ($current_sub, "*** The maximum number of executions per query has been reached, exiting early.");
    }
}
else {
    my_print ($current_sub, "*** The test window is complete.");
}
my_print ($current_sub, "There are still " . keys(%pids) . " queries executing.");

# Wait for the stragglers to complete.
while(keys(%pids) > 0) {
    my $pid = waitpid -1, WNOHANG;
        
    if ($pid > 0) {
        my $query_type = $pids{$pid};
        delete $pids{$pid};
        my $rc = $? >> 8;
        my $found_pid = complete_query($rc,$query_type);
        if($found_pid) {
            # pid successfully removed
            if($debug) { my_print ($current_sub, "COMPLETE:\t(Complete) pid $rc"); }
        }
        else {
            my_print ($current_sub, "****** Warning: Could not find Running pid $rc to complete.");
        }
        if($debug) { my_print ($current_sub, "END:\t(Finalize) Removing " . $rc . "(" . $pid . ")"); }
    }
}

my_print ($current_sub, "*** The last query has completed. Simulation ended.");

clean_up();

exit 1;
