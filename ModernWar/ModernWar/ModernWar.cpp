// ModernWar.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "NationState.h"
#include "NationValidator.h"
#include "TreasuryController.h"

int main()
{
    std::array<ModernWarCore::NationState, 2> roster = { { {1, "Nation A", 20, 0, 5}, {2, "Nation B", 20, 0, 8}} };

    ModernWarCore::WorldState world = { 1, roster };

    std::string Transfering_action = ModernWarCore::TransferTreasury(world, 1, 2, 7);

    if (Transfering_action.empty())
    {
        std::cout << "TransferTreasury: OK" << std::endl;
        std::cout << "TurnNumber= "  << world.TurnNumber << std::endl;
    }
    else
    {
        std::cout << "ERROR: " << Transfering_action << std::endl;
        return 1;
    }
    if (world.Nations[0].NationId < world.Nations[1].NationId)
    {
        std::cout << "NationId=" << world.Nations[0].NationId << "; DisplayName=" << world.Nations[0].DisplayName << "; Treasury=" << world.Nations[0].Treasury << "; StrategicScore=" 
                  << world.Nations[0].StrategicScore << "; CapitalTerritoryId=" << world.Nations[0].CapitalTerritoryId << std::endl;
        std::cout << "NationId=" << world.Nations[1].NationId << "; DisplayName=" << world.Nations[1].DisplayName << "; Treasury=" << world.Nations[1].Treasury << "; StrategicScore=" 
                  << world.Nations[1].StrategicScore << "; CapitalTerritoryId=" << world.Nations[1].CapitalTerritoryId << std::endl;
    }
    else
    {
        std::cout << "NationId=" << world.Nations[1].NationId << "; DisplayName=" << world.Nations[1].DisplayName << "; Treasury=" << world.Nations[1].Treasury << "; StrategicScore=" 
                  << world.Nations[1].StrategicScore << "; CapitalTerritoryId=" << world.Nations[1].CapitalTerritoryId << std::endl;
        std::cout << "NationId=" << world.Nations[0].NationId << "; DisplayName=" << world.Nations[0].DisplayName << "; Treasury=" << world.Nations[0].Treasury << "; StrategicScore=" 
                  << world.Nations[0].StrategicScore << "; CapitalTerritoryId=" << world.Nations[0].CapitalTerritoryId << std::endl;
    }
    

    return 0;
}

