#pragma once
#include <cstdint>
#include <string>

namespace ModernWarCore
{
	struct NationState
	{
		std::uint32_t NationId = 0;
		std::string DisplayName = "";
		std::int64_t Treasury = 0;
		std::int32_t StrategicScore = 0;
		std::uint32_t CapitalTerritoryId = 0;
	};
}