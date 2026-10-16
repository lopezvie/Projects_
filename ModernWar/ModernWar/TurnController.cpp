#include "TurnController.h"



namespace ModernWarCore
{
	std::string AdvanceTurn(WorldState& world)
	{
		std::string validate_test = ModernWarCore::ValidateRoster(world.Nations);
		if (!validate_test.empty())
			return validate_test;
		else
		{
			if (world.TurnNumber == 0)
				return "TurnNumber must be > 0";
			else if (world.TurnNumber == 4294967295)
				return "TurnNumber limit reached";
			else
			{
				std::string economy_settle = ModernWarCore::ApplyEconomy(world);
				if (economy_settle.empty())
				{
					world.TurnNumber++;
					return "";
				}
				else
					return economy_settle;
			}
		}
	}
}