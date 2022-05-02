#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"
#include <string>

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const
    {
        std::string output = std::string();
        std::string Content = "\"" + content + "\"";
        std::string input = "printf " + Content + " | sha256sum";
        
        FILE* fp;
        const unsigned int sizebuf = 2048;
        char buff[sizebuf];
        fp = popen(input.c_str(), "r");
        
        while (fgets(buff, sizeof(buff), fp)) {
            output += buff;
        }
        
        pclose(fp);
        
        return PageId(output.substr(0, output.length() - 4));
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
