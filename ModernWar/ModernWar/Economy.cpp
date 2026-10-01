#include "Economy.h"


namespace ModernWarCore
{
	std::string ApplyEconomy(WorldState& world)
	{
		std::string rooster_validation = ModernWarCore::ValidateRoster(world.Nations);
		if (!rooster_validation.empty())
			return rooster_validation;
		else
		{
			ModernWarCore::WorldState world_copy = world; 

			for (auto nation :  world_copy.Nations)
			{
				if (nation.IncomePerTurn > 0)
				{
					std::string credit_result = ModernWarCore::CreditTreasury(world_copy, nation.NationId, nation.IncomePerTurn);
					if (!credit_result.empty())
						return credit_result;
				}
				if (nation.ExpensesPerTurn > 0)
				{
					std::string spend_result = ModernWarCore::SpendTreasury(world_copy, nation.NationId, nation.ExpensesPerTurn);
					if (!spend_result.empty())
						return spend_result;
				}
			}

			world = world_copy;
			return "";
		}
	}
}