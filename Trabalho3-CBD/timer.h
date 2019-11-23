#ifndef TIMER_H
#define TIMER_H
#include <ctime>

class Timer {
    std::clock_t start_time;

public:
    /**
     * Começa o timer
     */
    void start();
    
    /**
     * @return o tempo passado, em segundos
     */
    double getElapsedTime();
};

void Timer::start() {
    start_time = std::clock();
}

double Timer::getElapsedTime() {
    return ( std::clock() - start_time ) / (double) CLOCKS_PER_SEC;
}

#endif //TIMER_H