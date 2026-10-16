// ModernWar.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <limits>

#include "NationState.h"
#include "NationValidator.h"
#include "TreasuryController.h"
#include "TurnController.h"

typedef struct inputFromUser {
    std::string command;
    std::string s_sourceId;;
    std::string s_destinationId;
    std::string s_amount;
    std::uint32_t i_sourceId = 0;
    std::uint32_t i_destinationId = 0;
    std::uint64_t i_amount = 0;
    int tokens_number = 0;
} Entry;

bool pasing_commands(Entry& entry, const std::string& commandEntry, const std::string& raw_entry)
{
    for (int i = 0; i < raw_entry.size() - 1; i++)
    {
        if ((isdigit(raw_entry[i]) || isalpha(raw_entry[i])) && (raw_entry[ i + 1 ] == ' '))
            entry.tokens_number++;
    }
    if (entry.tokens_number >= 5)
    {
        entry.tokens_number = 0;
        return false;
    }
    entry.tokens_number = 0;
    for (auto c : raw_entry)
    {
        if (!isdigit(c) && c != ' ' && !isalpha(c) && c != '\t')
            return false;
    }
    for (auto c : entry.s_amount)
    {
        if (!isdigit(c))
            return false;
    }
    for (auto c : entry.s_sourceId)
    {
        if (!isdigit(c))
            return false;
    }
    for (auto c : entry.s_destinationId)
    {
        if (!isdigit(c))
            return false;
    }
    if (entry.command != commandEntry)
        return false;
        
    try
    {
        std::uint64_t check_numeric_bounderies = std::stoll(entry.s_sourceId);
        if (check_numeric_bounderies > std::numeric_limits<std::uint32_t>::max())
            return false;
        check_numeric_bounderies = std::stoll(entry.s_destinationId);
        if (check_numeric_bounderies > std::numeric_limits<std::uint32_t>::max())
            return false;
        entry.i_sourceId = std::stoul(entry.s_sourceId);
        entry.i_destinationId = std::stoul(entry.s_destinationId);
        entry.i_amount = std::stoull(entry.s_amount);
        if (entry.i_amount < 0 || entry.i_amount > 9223372036854775807)
            return false;
        return true;
    }
    catch (...)
    {
        return false;
    }
}

bool tokens_number_consumer(std::istringstream& iss, const std::string command, const int number_allowed_tokens)
{
    std::string _consumer;
    int _tokens_consummed = 0;
    while (iss >> _consumer)
    {
        if (_consumer != " ")
            _tokens_consummed++;
    }
    iss.clear();
    iss.str(command);
    if (_tokens_consummed > number_allowed_tokens)
        return false;
    else
        return true;
}

void reporting_helper(const ModernWarCore::WorldState& world)
{
    std::vector<ModernWarCore::NationState> nations = world.Nations;

    std::sort(nations.begin(), nations.end(), [](const ModernWarCore::NationState& a, const ModernWarCore::NationState& b) {
        return a.NationId < b.NationId;
        });
    std::cout << "TurnNumber=" << world.TurnNumber << std::endl;
    for (const auto& nation : nations) {

        std::cout << "NationId=" << nation.NationId << "; Treasury=" << nation.Treasury << std::endl;
    }
}

int main()
{
    const std::vector<ModernWarCore::NationState> roster = { {83, "Nation C", 30, 0, 11, 4, 1}, {17, "Nation A", 20, 0, 5, 7, 3 }, { 42, "Nation B", 10, 0, 8, 2, 5 }};

    ModernWarCore::WorldState world = { 1, roster };

    reporting_helper(world);

    std::string command = "";

    Entry user_entry;

    while (std::getline(std::cin, command))
    {
        std::istringstream iss(command);

        if (command == "quit")
        {
            std::cout << "Goodbye\n";
            return 0;
        }
        else if (command == "end")
        {
            std::string advancing_attempt = ModernWarCore::AdvanceTurn(world);
            if (advancing_attempt.empty())
            {
                std::cout << "AdvanceTurn: OK" << std::endl;
                reporting_helper(world);
            }
            else
            {
                std::cout << "ERROR: " << advancing_attempt << std::endl;
                reporting_helper(world);
            }
        }
        else if (tokens_number_consumer(iss, command, 4) && iss >> user_entry.command >> user_entry.s_sourceId >> user_entry.s_destinationId >> user_entry.s_amount)
        {
            if (pasing_commands(user_entry, "transfer", command))
            {
                std::string transfer_attempt = ModernWarCore::TransferTreasury(world, user_entry.i_sourceId, user_entry.i_destinationId, user_entry.i_amount);

                if (transfer_attempt.empty())
                {
                    std::cout << "TransferTreasury: OK" << std::endl;
                    reporting_helper(world);
                }
                else
                {
                    std::cout << "ERROR: " << transfer_attempt << std::endl;
                    reporting_helper(world);
                }
            }
            else
            {
                std::cout << "ERROR: Invalid transfer command" << std::endl; 
            }
            
        }
        else
            std::cout << "ERROR: Unknown command\n";
    }

    return 0;
}
    


