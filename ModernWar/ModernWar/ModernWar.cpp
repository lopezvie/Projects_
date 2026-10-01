// ModernWar.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "NationState.h"
#include "NationValidator.h"
#include "TreasuryController.h"
#include "TurnController.h"

int main()
{
    std::array<ModernWarCore::NationState, 2> roster = { { {17, "Nation A", 20, 0, 5, 7, 3}, {42, "Nation B", 10, 0, 8, 0, 11}} };

    ModernWarCore::WorldState world = { 1, roster };

    std::string turn_action = ModernWarCore::AdvanceTurn(world);

    if (turn_action.empty())
    {
        std::cout << "TransferTreasury: OK" << std::endl;
        std::cout << "TurnNumber= "  << world.TurnNumber << std::endl;
    }
    else
    {
        std::cout << "ERROR: " << turn_action << std::endl;
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

