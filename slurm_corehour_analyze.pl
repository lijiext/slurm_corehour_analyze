#!/usr/bin/perl
use strict;
use warnings;
use POSIX qw(strftime);
use Getopt::Long qw(GetOptions);
use Time::Piece;
use Time::Seconds;

# 初始化日志文件
my $log_file = "slurm_core_usage_analyze.log";
open my $log_fh, '>>', $log_file or die "Could not open log file '$log_file' $!\n";
select((select($log_fh), $| = 1)[0]);  # 使日志文件句柄立即刷新

# 自定义日志子程序
sub log_msg {
    my ($level, $msg) = @_;
    my $timestamp = strftime('%Y-%m-%d %H:%M:%S', localtime);
    my $formatted_msg = "[$timestamp] [$level] $msg\n";

    # 输出到日志文件和控制台
    print $log_fh $formatted_msg;
    print STDOUT $formatted_msg;
}

# 类定义
{
    package ReportGenerator;
    use POSIX qw(strftime);
    use Getopt::Long qw(GetOptions);
    use Time::Piece;
    use Time::Seconds;

    sub new {
        my ($class) = @_;
        my $self = {
            total_start_date       => '2022-01-01',
            end_date               => strftime('%Y-%m-%d', localtime),
            report_days            => 7,
            sacct_output_file      => '',
            report_output_file     => '',
            user_partition_usage   => {},
        };
        bless $self, $class;
        return $self;
    }

    sub parse_arguments {
        my ($self) = @_;
        GetOptions(
            'end-date=s' => \$self->{end_date},
            'days=i'     => \$self->{report_days},
        ) or die "Error in command line arguments\n";
        main::log_msg("INFO", "Parsed arguments: end_date=$self->{end_date}, report_days=$self->{report_days}");
    }

    sub initialize_files {
        my ($self) = @_;
        $self->{report_start_date} = strftime('%Y-%m-%d', localtime(time - $self->{report_days} * 86400));
        $self->{sacct_output_file} = "sacct_output_$self->{total_start_date}_to_$self->{end_date}.txt";
        $self->{report_output_file} = "core_usage_report_$self->{end_date}.csv";
        main::log_msg("INFO", "Initialized files. Report start date: $self->{report_start_date}, sacct output file: $self->{sacct_output_file}, report output file: $self->{report_output_file}");
    }

    sub run_sacct_command {
        my ($self) = @_;
        my $sacct_format_fields = "JobName,User,Partition,Nodelist,Start,End,State,CPUTimeRAW,JobID";
        my $end_datetime = $self->{end_date} . "T23:59:59"; # 结束时间设为当天的23:59:59
        my $sacct_command = "sacct -S $self->{total_start_date} -E $end_datetime --format $sacct_format_fields -n -P > $self->{sacct_output_file}";
        main::log_msg("INFO", "Running sacct command: $sacct_command");
        system($sacct_command) == 0 or die "Failed to run sacct command: $!\n";
        main::log_msg("INFO", "sacct command completed, data stored in $self->{sacct_output_file}");
    }

    sub load_sacct_data {
        my ($self) = @_;
        open my $sacct_fh, '<', $self->{sacct_output_file} or die "Could not open '$self->{sacct_output_file}' $!\n";
        main::log_msg("INFO", "Reading data from $self->{sacct_output_file}");
        $self->{sacct_lines} = [<$sacct_fh>];
        close $sacct_fh;
    }

    sub process_data {
        my ($self) = @_;
        foreach my $line (@{$self->{sacct_lines}}) {
            chomp $line;
            my ($job_name, $user, $partition, $nodelist, $start_time_str, $end_time_str, $state, $cpu_time_raw, $job_id) = split(/\|/, $line);

            # 确保 JobID 是字符串并过滤掉包含 '.' 的作业步骤
            $job_id = "$job_id";
            next if $job_id =~ /\./;

            # 计算核心小时数
            my $core_hours = $cpu_time_raw / 3600.0;

            my ($start_time, $end_time);
            eval {
                # 转换时间字符串为时间对象
                if ($start_time_str ne 'None') {
                    $start_time = Time::Piece->strptime($start_time_str, "%Y-%m-%dT%H:%M:%S");
                }
                $end_time = ($end_time_str eq 'Unknown' || $end_time_str eq 'EndTime' || $end_time_str eq 'None')
                    ? Time::Piece->strptime("$self->{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S")
                    : Time::Piece->strptime($end_time_str, "%Y-%m-%dT%H:%M:%S");
            };
            if ($@) {
                main::log_msg("WARN", "Error parsing time for job $job_id: $@");
                next;
            }

            # 如果开始时间或结束时间解析失败，跳过该作业
            unless ($start_time && $end_time) {
                main::log_msg("WARN", "Skipping job $job_id due to invalid start or end time.");
                next;
            }

            # 检查作业是否在报告的时间范围内
            my $report_start_time = Time::Piece->strptime("$self->{report_start_date} 00:00:00", "%Y-%m-%d %H:%M:%S");
            my $report_end_time = Time::Piece->strptime("$self->{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S");

            my $total_core_hours = 0;
            my $in_range_core_hours = 0;

            # 计算总的核心小时数
            $total_core_hours += $core_hours;

            # 检查时间跨度以避免除以零错误
            if (($end_time - $start_time) == 0) {
                main::log_msg("WARN", "Skipping job $job_id due to zero duration.");
                next;
            }

            # 计算在报告时间范围内的核心小时数
            if ($end_time >= $report_start_time && $start_time <= $report_end_time) {
                # 作业跨时间范围处理
                my $overlap_start_time = $start_time > $report_start_time ? $start_time : $report_start_time;
                my $overlap_end_time = $end_time < $report_end_time ? $end_time : $report_end_time;
                my $overlap_duration = $overlap_end_time - $overlap_start_time;
                $in_range_core_hours += $overlap_duration->hours * ($core_hours / (($end_time - $start_time)->hours));
            }

            # 累加用户和分区的核心小时数
            $self->{user_partition_usage}{$user}{$partition}{total} += $total_core_hours;
            $self->{user_partition_usage}{$user}{$partition}{in_range} += $in_range_core_hours;
        }
    }

    sub generate_report {
        my ($self) = @_;
        open my $report_fh, '>', $self->{report_output_file} or die "Could not open '$self->{report_output_file}' $!\n";
        print $report_fh "User,Partition,TotalCoreHoursUsed,CoreHoursUsedInRange\n";

        foreach my $user (keys %{$self->{user_partition_usage}}) {
            foreach my $partition (keys %{$self->{user_partition_usage}{$user}}) {
                my $total_core_hours = $self->{user_partition_usage}{$user}{$partition}{total} // 0;
                my $in_range_core_hours = $self->{user_partition_usage}{$user}{$partition}{in_range} // 0;
                print $report_fh "$user,$partition,$total_core_hours,$in_range_core_hours\n";
            }
        }

        close $report_fh;
        main::log_msg("INFO", "Report saved to $self->{report_output_file}");
    }
}

# 主程序
my $report_generator = ReportGenerator->new();
main::log_msg("INFO", "========CORE/HOUR USAGE ANALYZE START========");
$report_generator->parse_arguments();
$report_generator->initialize_files();
$report_generator->run_sacct_command();
$report_generator->load_sacct_data();
$report_generator->process_data();
$report_generator->generate_report();
main::log_msg("INFO", "========CORE/HOUR USAGE ANALYZE END========");
# 关闭日志文件句柄
close $log_fh;
