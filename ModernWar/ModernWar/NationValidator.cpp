#include "NationValidator.h"

namespace ModernWarCore
{

	std::string ValidateNation(const NationState& nation)
	{
		if (nation.NationId == 0)
			return "NationId must be > 0";
		else if (nation.DisplayName.empty())
			return "DisplayName must not be empty";
		else if (nation.Treasury < 0)
			return "Treasury must be >= 0";
		else if (nation.StrategicScore < 0)
			return "StrategicScore must be >= 0";
		else if (nation.CapitalTerritoryId == 0)
			return "CapitalTerritoryId must be > 0";
		else if (nation.IncomePerTurn < 0)
			return "IncomePerTurn must be >= 0";
		else if (nation.ExpensesPerTurn < 0)
			return "ExpensesPerTurn must be >= 0";
		else
			return "";
	}

	std::string ValidateRoster(const std::vector<NationState>& nations)
	{
		if (nations.empty())
		{
			return "Error: Invalid Startup Rooster";
		}
		for (auto position = 0; position < nations.size(); position++)
		{
			std::string result = ModernWarCore::ValidateNation(nations[position]);
			if (!result.empty())
				return "Roster[" + std::to_string(position) + "]: " + result;
			std::uint32_t id_to_check = nations[position].NationId;
			if (position != 0)
			{
				for (auto inner_position = 0; inner_position < position; inner_position++)
				{
					if (nations[inner_position].NationId == id_to_check)
					{
						return "Duplicate NationId";
					}
				}
			}
			id_to_check = nations[position].CapitalTerritoryId;
			if (position != 0)
			{
				for (auto inner_position = 0; inner_position < position; inner_position++)
				{
					if (nations[inner_position].CapitalTerritoryId == id_to_check)
					{
						return "Duplicate CapitalTerritoryId";;
					}
				}
			}
		}

		return "";
	}
}