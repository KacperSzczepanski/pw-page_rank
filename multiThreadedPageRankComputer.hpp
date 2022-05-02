#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <future>
#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"
#include "singleThreadedPageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network, double alpha, uint32_t iterations, double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap, previousPageHashMap;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::unordered_set<PageId, PageIdHash> danglingNodes;
        double dangleSum = 0;
        double difference = 0;
        Barrier barrier(numThreads);
        std::unordered_map<PageId, PageRank, PageIdHash>::iterator mapIter;
        std::unordered_set<PageId, PageIdHash>::iterator setIter;
        std::mutex calculationGuard, iteratorGuard;

        for (auto const& page : network.getPages()) {
            page.generateId(network.getGenerator());
            pageHashMap[page.getId()] = 1.0 / network.getSize();
        }
        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.insert(page.getId());
            }

            numLinks[page.getId()] = page.getLinks().size();

            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        previousPageHashMap = pageHashMap;
        
        std::vector<std::thread> threads;
        for (uint32_t thread = 0; thread < numThreads; ++thread) {
            uint32_t threadNumber = thread;

            threads.emplace_back(std::thread {[&, threadNumber, alpha, iterations, tolerance]() {
                computePageRanks(threadNumber,
                                 previousPageHashMap, pageHashMap,
                                 edges, numLinks, danglingNodes,
                                 barrier,
                                 alpha, iterations, tolerance, network.getSize(),
                                 dangleSum, difference,
                                 mapIter, setIter,
                                 calculationGuard, iteratorGuard);
            } });
        }
        for (uint32_t thread = 0; thread < numThreads; ++thread) {
            threads[thread].join();
        }
        threads.clear();

        std::vector<PageIdAndRank> result;
        for (auto iter : pageHashMap) {
            result.push_back(PageIdAndRank(iter.first, iter.second));
        }

        ASSERT(result.size() == network.getSize(), "Invalid result size=" << result.size() << ", for network" << network);

        return result;
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;

    class Barrier {
    private:
        uint32_t resistance;
        uint32_t initialResistance;
        uint32_t reachedCounter;
        std::condition_variable cv;
        std::mutex cv_m;

    public:
        Barrier(int res)
            : resistance(res)
            , initialResistance(res)
            , reachedCounter(0)
        {}

        void reach()
        {
            std::unique_lock<std::mutex> lk(cv_m);
            --resistance;
            uint32_t checkReach = reachedCounter;

            if (resistance == 0) {
                ++reachedCounter;
                resistance = initialResistance;
                cv.notify_all();
            } else {
                cv.wait(lk, [this, checkReach] { return checkReach != reachedCounter; });
            }
        }
    };

    void computePageRanks(const uint32_t id,
        std::unordered_map<PageId, PageRank, PageIdHash>& previousPageHashMap,
        std::unordered_map<PageId, PageRank, PageIdHash>& pageHashMap,
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash>& edges,
        std::unordered_map<PageId, uint32_t, PageIdHash>& numLinks,
        std::unordered_set<PageId, PageIdHash>& danglingNodes,
        Barrier& barrier,
        const double alpha, const uint32_t iterations, const double tolerance, const uint32_t networkSize,
        double& dangleSum, double& difference,
        std::unordered_map<PageId, PageRank, PageIdHash>::iterator& mapIter,
        std::unordered_set<PageId, PageIdHash>::iterator& setIter,
        std::mutex& calculationGuard, std::mutex& iteratorGuard) const {
        
        double partialDangle, partialDiff;
        std::unordered_map<PageId, PageRank, PageIdHash>::iterator helpMapIter;
        std::unordered_set<PageId, PageIdHash>::iterator helpSetIter;

        for (uint32_t i = 0; i < iterations; ++i) {
            barrier.reach();

            if (id == 0) {
                dangleSum = 0;
                difference = 0;

                previousPageHashMap = pageHashMap;
                mapIter = pageHashMap.begin();
                setIter = danglingNodes.begin();
            }
            partialDangle = 0;
            partialDiff = 0;

            barrier.reach();

            while (setIter != danglingNodes.end()) {
                {
                    std::lock_guard <std::mutex> lock(iteratorGuard);
                    if (setIter == danglingNodes.end()) {
                        break;
                    }

                    helpSetIter = setIter;
                    ++setIter;
                }

                partialDangle += pageHashMap[*helpSetIter];
            }
            {
                std::lock_guard <std::mutex> lock(calculationGuard);
                dangleSum += partialDangle;
            }

            barrier.reach();
            if (id == 0) {
                dangleSum *= alpha;
            }
            barrier.reach();

            while (mapIter != pageHashMap.end()) {
                {
                    std::lock_guard <std::mutex> lock(iteratorGuard);
                    if (mapIter == pageHashMap.end()) {
                        break;
                    }

                    helpMapIter = mapIter;
                    ++mapIter;
                }

                PageId pageId = helpMapIter->first;

                double oldRank = pageHashMap[pageId];
                double newRank = (dangleSum + 1.0 - alpha) / networkSize;

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        newRank += previousPageHashMap[link] * alpha / numLinks[link];
                    }
                }

                pageHashMap[pageId] = newRank;

                partialDiff += std::abs(newRank - oldRank);
            }
            {
                std::lock_guard<std::mutex> lock(calculationGuard);
                difference += partialDiff;
            }

            barrier.reach();

            if (difference < tolerance) {
                return;
            }
        }
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
