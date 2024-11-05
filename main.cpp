#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <algorithm>
#include <sstream>
#include <string>
#include <map>
#include <iomanip>
#include <cstdint>

// Struct to hold job details
struct Job {
    int jobId;
    int arrivalDay;
    int arrivalHour;
    int memReq;    // in GB
    int cpuReq;    // number of cores
    int exeTime;   // in hours
    long long arrivalTime; // total hours since day 0, hour 0
    long long finishTime;  // total hours since day 0, hour 0

    Job(int id, int day, int hour, int mem, int cpu, int exe)
        : jobId(id), arrivalDay(day), arrivalHour(hour), memReq(mem), cpuReq(cpu), exeTime(exe) {
        arrivalTime = day * 24 + hour;
        finishTime = -1; // Not yet scheduled
    }
};

// Struct to hold worker node details
struct Worker {
    int workerId;
    int totalCores;
    int totalMem; // in GB
    int availableCores;
    int availableMem; // in GB
    std::vector<std::pair<long long, Job>> runningJobs;

    Worker(int id, int cores = 24, int mem = 64)
        : workerId(id), totalCores(cores), totalMem(mem), availableCores(cores), availableMem(mem) {}
};

// Struct to hold utilization data per day
struct UtilizationRecord {
    std::string queuePolicy;
    std::string workerPolicy;
    std::vector<double> dailyCpuUtilization;
    std::vector<double> dailyMemUtilization;
};

// Function to trim whitespace from a string
std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    size_t end = s.find_last_not_of(" \t\r\n");
    if (start == std::string::npos || end == std::string::npos)
        return "";
    return s.substr(start, end - start + 1);
}

// Function to parse a line from JobArrival.txt
bool parseJobLine(const std::string& line, Job& job) {
    std::istringstream iss(line);
    std::string label;
    int id, day, hour, mem, cpu, exe;

    if (!(iss >> label >> id)) return false;        // JobId
    if (!(iss >> label >> day)) return false;       // Arrival Day
    if (!(iss >> label >> hour)) return false;      // Time Hour
    if (!(iss >> label >> mem)) return false;       // MemReq
    if (!(iss >> label >> cpu)) return false;       // CPUReg
    if (!(iss >> label >> exe)) return false;       // ExeTime

    job = Job(id, day, hour, mem, cpu, exe);
    return true;
}

// Function to load jobs from JobArrival.txt
std::vector<Job> loadJobs(const std::string& filename) {
    std::vector<Job> jobs;
    std::ifstream infile(filename);
    if (!infile.is_open()) {
        std::cerr << "Error: Unable to open " << filename << "\n";
        return jobs;
    }

    std::string line;
    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        Job job(0, 0, 0, 0, 0, 0);
        if (parseJobLine(line, job)) {
            jobs.push_back(job);
        }
    }
    infile.close();
    return jobs;
}

// Queueing policy comparator functions
bool compareFCFS(const Job& a, const Job& b) {
    return a.arrivalTime == b.arrivalTime ? a.jobId < b.jobId : a.arrivalTime < b.arrivalTime;
}

bool compareSmallestJobFirst(const Job& a, const Job& b) {
    long long grossA = static_cast<long long>(a.exeTime) * a.cpuReq * a.memReq;
    long long grossB = static_cast<long long>(b.exeTime) * b.cpuReq * b.memReq;
    return grossA == grossB ? a.jobId < b.jobId : grossA < grossB;
}

bool compareShortestDurationFirst(const Job& a, const Job& b) {
    return a.exeTime == b.exeTime ? a.jobId < b.jobId : a.exeTime < b.exeTime;
}

// Function to sort the job queue based on the queueing policy
void sortJobQueue(std::vector<Job>& jobQueue, const std::string& queuePolicy) {
    if (queuePolicy == "fcfs") {
        std::sort(jobQueue.begin(), jobQueue.end(), compareFCFS);
    } else if (queuePolicy == "smallest_job_first") {
        std::sort(jobQueue.begin(), jobQueue.end(), compareSmallestJobFirst);
    } else if (queuePolicy == "shortest_duration_first") {
        std::sort(jobQueue.begin(), jobQueue.end(), compareShortestDurationFirst);
    }
}

// Function to find a worker based on the worker selection policy
Worker* findWorkerNode(std::vector<Worker>& workers, const Job& job, const std::string& workerPolicy) {
    Worker* selectedWorker = nullptr;
    if (workerPolicy == "first_fit") {
        for (auto& worker : workers) {
            if (worker.availableCores >= job.cpuReq && worker.availableMem >= job.memReq) {
                selectedWorker = &worker;
                break;
            }
        }
    } else if (workerPolicy == "best_fit") {
        int minResidual = INT32_MAX;
        for (auto& worker : workers) {
            if (worker.availableCores >= job.cpuReq && worker.availableMem >= job.memReq) {
                int residual = (worker.availableCores - job.cpuReq) + (worker.availableMem - job.memReq);
                if (residual < minResidual) {
                    minResidual = residual;
                    selectedWorker = &worker;
                }
            }
        }
    } else if (workerPolicy == "worst_fit") {
        int maxResidual = -1;
        for (auto& worker : workers) {
            if (worker.availableCores >= job.cpuReq && worker.availableMem >= job.memReq) {
                int residual = (worker.availableCores - job.cpuReq) + (worker.availableMem - job.memReq);
                if (residual > maxResidual) {
                    maxResidual = residual;
                    selectedWorker = &worker;
                }
            }
        }
    }
    return selectedWorker;
}

// Function to simulate scheduling for a specific policy combination
void simulate(const std::vector<Job>& allJobs, const std::string& queuePolicy, const std::string& workerPolicy, UtilizationRecord& record) {
    // Initialize workers
    std::vector<Worker> workers;
    for (int i = 0; i < 128; ++i) {
        workers.emplace_back(i);
    }

    // Create a copy of all jobs and sort them by arrival time
    std::vector<Job> jobs = allJobs;
    std::sort(jobs.begin(), jobs.end(), compareFCFS);

    // Sort jobs based on queueing policy
    sortJobQueue(jobs, queuePolicy);

    // Initialize job queue
    std::vector<Job> jobQueue; // Make sure jobQueue is defined here

    // Initialize current time
    long long currentTime = 0;

    // Determine the simulation end time
    long long endTime = 0;
    for (auto& job : jobs) {
        endTime = std::max(endTime, static_cast<long long>(job.arrivalTime + job.exeTime));
    }

    // Initialize utilization tracking
    std::map<int, std::pair<long long, long long>> dailyUtilization; // day -> (totalCoresUsed, totalMemUsed)
    int currentDay = 0;

    // Initialize a job index
    size_t jobIndex = 0;

    // Simulation loop
    for (currentTime = 0; currentTime <= endTime; ++currentTime) {
        // Check for day change
        int day = currentTime / 24;
        if (day != currentDay) {
            currentDay = day;
        }

        // Release resources from completed jobs
        for (auto& worker : workers) {
            std::vector<std::pair<long long, Job>> remainingJobs;
            for (auto& runningJob : worker.runningJobs) {
                if (runningJob.first <= currentTime) {
                    // Job completed, free resources
                    worker.availableCores += runningJob.second.cpuReq;
                    worker.availableMem += runningJob.second.memReq;
                } else {
                    remainingJobs.push_back(runningJob);
                }
            }
            worker.runningJobs = remainingJobs;
        }

        // Add new arriving jobs to the job queue
        while (jobIndex < jobs.size() && jobs[jobIndex].arrivalTime == currentTime) {
            jobQueue.push_back(jobs[jobIndex]);
            jobIndex++;
        }

        // Sort the job queue based on queueing policy
        sortJobQueue(jobQueue, queuePolicy);

        // Try to schedule jobs
        std::vector<Job> remainingQueue;
        for (auto& job : jobQueue) {
            Worker* worker = findWorkerNode(workers, job, workerPolicy);
            if (worker != nullptr) {
                // Schedule the job
                worker->availableCores -= job.cpuReq;
                worker->availableMem -= job.memReq;
                Job scheduledJob = job;
                scheduledJob.finishTime = currentTime + job.exeTime;
                worker->runningJobs.emplace_back(scheduledJob.finishTime, scheduledJob);
            } else {
                // Re-queue the job
                remainingQueue.push_back(job);
            }
        }
        jobQueue = remainingQueue; // Update jobQueue with remaining jobs

        // Calculate utilization for the current hour
        long long totalCoresUsed = 0;
        long long totalMemUsed = 0;
        for (auto& worker : workers) {
            totalCoresUsed += (worker.totalCores - worker.availableCores);
            totalMemUsed += (worker.totalMem - worker.availableMem);
        }

        // Accumulate utilization per day
        dailyUtilization[day].first += totalCoresUsed;
        dailyUtilization[day].second += totalMemUsed;
    }

    // Calculate average utilization per day
    for (auto& entry : dailyUtilization) {
        int day = entry.first;
        long long coresUsed = entry.second.first;
        long long memUsed = entry.second.second;
        double avgCpu = static_cast<double>(coresUsed) / (128 * 24) * 100.0; // Percentage
        double avgMem = static_cast<double>(memUsed) / (128 * 64) * 100.0;  // Percentage
        record.dailyCpuUtilization.push_back(avgCpu);
        record.dailyMemUtilization.push_back(avgMem);
    }

    // Set policy names
    record.queuePolicy = queuePolicy;
    record.workerPolicy = workerPolicy;
}

int main() {
    std::vector<Job> allJobs = loadJobs("JobArrival.txt");
    if (allJobs.empty()) {
        std::cerr << "No jobs loaded. Exiting.\n";
        return 1;
    }

    UtilizationRecord record;
    record.queuePolicy = "fcfs";
    record.workerPolicy = "first_fit";
    simulate(allJobs, record.queuePolicy, record.workerPolicy, record);

    std::cout << "Daily CPU Utilization:\n";
    for (double cpuUsage : record.dailyCpuUtilization) {
        std::cout << cpuUsage * 100 << "%\n";
    }

    std::cout << "Daily Memory Utilization:\n";
    for (double memUsage : record.dailyMemUtilization) {
        std::cout << memUsage * 100 << "%\n";
    }

    return 0;
}