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
		else
			return "";
	}

	std::string ValidateRoster(const std::array<NationState, 2>& nations)
	{
		std::string result1 = ModernWarCore::ValidateNation(nations[0]);
		if (!result1.empty())
			return "Roster[0]: " + result1;
		else
		{
			std::string result2 = ModernWarCore::ValidateNation(nations[1]);
			if (!result2.empty())
				return "Roster[1]: " + result2;
			else if (nations[0].NationId == nations[1].NationId)
				return "Duplicate NationId";
			else if (nations[0].CapitalTerritoryId == nations[1].CapitalTerritoryId)
				return "Duplicate CapitalTerritoryId";
			else
				return "";
		}
	}
}