#pragma once
#include <string>
#include <cstdint>
#include <limits>
#include <algorithm>

#include "WorldState.h"
#include "NationValidator.h"
#include "NationState.h"

namespace ModernWarCore
{
	std::string SpendTreasury(WorldState& world, std::uint32_t nationId, std::int64_t amount);
	std::string CreditTreasury(WorldState& world, std::uint32_t nationId, std::int64_t amount);
	std::string TransferTreasury(WorldState& world, std::uint32_t sourceNationId, std::uint32_t destinationNationId, std::int64_t amount);
	NationState* FindNation(WorldState& world, std::uint32_t nationId);
	const NationState* FindNation(const WorldState& world, std::uint32_t nationId);
}