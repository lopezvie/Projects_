#pragma once
#include <cstdint>
#include <array>
#include <vector>

#include "NationState.h"


namespace ModernWarCore
{
	struct WorldState
	{
		std::uint32_t TurnNumber = 1;
		std::vector<NationState> Nations;
	};
}